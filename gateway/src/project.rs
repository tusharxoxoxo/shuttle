use std::collections::{HashMap, VecDeque};
use std::convert::{identity, Infallible};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use bollard::container::{
    Config, CreateContainerOptions, KillContainerOptions, RemoveContainerOptions,
    StopContainerOptions,
};
use bollard::errors::Error as DockerError;
use bollard::models::{ContainerInspectResponse, ContainerStateStatusEnum};
use bollard::network::{ConnectNetworkOptions, DisconnectNetworkOptions};
use bollard::service::MountTypeEnum;
use bollard::system::EventsOptions;
use bollard::volume::RemoveVolumeOptions;
use fqdn::FQDN;
use futures::prelude::*;
use http::header::AUTHORIZATION;
use http::uri::InvalidUri;
use http::{Method, Request, Uri};
use hyper::client::HttpConnector;
use hyper::{Body, Client};
use once_cell::sync::Lazy;
use rand::distributions::{Alphanumeric, DistString};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use shuttle_common::backends::headers::{X_SHUTTLE_ACCOUNT_NAME, X_SHUTTLE_ADMIN_SECRET};
use shuttle_common::constants::{default_idle_minutes, DEFAULT_IDLE_MINUTES};
use shuttle_common::models::project::ProjectName;
use shuttle_common::models::service;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, instrument, trace, warn};
use ulid::Ulid;
use uuid::Uuid;

use crate::service::ContainerSettings;
use crate::{DockerContext, Error, ErrorKind, IntoTryState, Refresh, State, TryState};

macro_rules! safe_unwrap {
    {$fst:ident$(.$attr:ident$(($ex:expr))?)+} => {
        $fst$(
            .$attr$(($ex))?
                .as_ref()
                .ok_or_else(|| ProjectError::internal(
                    concat!("container state object is malformed at attribute: ", stringify!($attr))
                ))?
        )+
    }
}

macro_rules! deserialize_json {
    {$ty:ty: $($json:tt)+} => {{
        let __ty_json = serde_json::json!($($json)+);
        serde_json::from_value::<$ty>(__ty_json).unwrap()
    }};
    {$($json:tt)+} => {{
        let __ty_json = serde_json::json!($($json)+);
        serde_json::from_value(__ty_json).unwrap()
    }}
}

macro_rules! impl_from_variant {
    {$e:ty: $($s:ty => $v:ident $(,)?)+} => {
        $(
            impl From<$s> for $e {
                fn from(s: $s) -> $e {
                    <$e>::$v(s)
                }
            }
        )+
    };
}

const RUNTIME_API_PORT: u16 = 8001;
const MAX_RECREATES: usize = 5;
const MAX_RESTARTS: usize = 5;
const MAX_REBOOTS: usize = 3;

// Client used for health checks
static CLIENT: Lazy<Client<HttpConnector>> = Lazy::new(Client::new);
// Health check must succeed within 10 seconds
pub static IS_HEALTHY_TIMEOUT: Duration = Duration::from_secs(10);

#[async_trait]
impl<Ctx> Refresh<Ctx> for ContainerInspectResponse
where
    Ctx: DockerContext,
{
    type Error = DockerError;
    async fn refresh(self, ctx: &Ctx) -> Result<Self, Self::Error> {
        ctx.docker()
            .inspect_container(self.id.as_ref().unwrap(), None)
            .await
    }
}

pub trait ContainerInspectResponseExt {
    fn container(&self) -> &ContainerInspectResponse;

    fn project_name(&self) -> Result<ProjectName, ProjectError> {
        let container = self.container();

        safe_unwrap!(container.config.labels.get("shuttle.project"))
            .to_string()
            .parse::<ProjectName>()
            .map_err(|_| ProjectError::internal("invalid project name"))
    }

    fn project_id(&self) -> Result<Ulid, ProjectError> {
        let container = self.container();
        Ulid::from_string(safe_unwrap!(container
            .config
            .labels
            .get("shuttle.project_id")))
        .map_err(|_| ProjectError::internal("invalid project id"))
    }

    fn idle_minutes(&self) -> u64 {
        let container = self.container();

        if let Some(config) = &container.config {
            if let Some(labels) = &config.labels {
                if let Some(idle_minutes) = labels.get("shuttle.idle_minutes") {
                    return idle_minutes.parse::<u64>().unwrap_or(DEFAULT_IDLE_MINUTES);
                }
            }
        }

        DEFAULT_IDLE_MINUTES
    }

    fn find_arg_and_then<'s, F, O>(&'s self, find: &str, and_then: F) -> Result<O, ProjectError>
    where
        F: FnOnce(&'s str) -> O,
        O: 's,
    {
        let mut args = self.args()?.iter();
        let out = if args.any(|arg| arg.as_str() == find) {
            args.next().map(|s| and_then(s.as_str()))
        } else {
            None
        };
        out.ok_or_else(|| ProjectError::internal(format!("no such argument: {find}")))
    }

    fn args(&self) -> Result<&Vec<String>, ProjectError> {
        let container = self.container();
        Ok(safe_unwrap!(container.args))
    }

    fn fqdn(&self) -> Result<FQDN, ProjectError> {
        self.find_arg_and_then("--proxy-fqdn", identity)?
            .parse()
            .map_err(|_| ProjectError::internal("invalid value for --proxy-fqdn"))
    }

    fn initial_key(&self) -> Result<String, ProjectError> {
        self.find_arg_and_then("--admin-secret", str::to_owned)
    }
}

impl ContainerInspectResponseExt for ContainerInspectResponse {
    fn container(&self) -> &ContainerInspectResponse {
        self
    }
}

impl From<DockerError> for Error {
    fn from(err: DockerError) -> Self {
        error!(error = %err, "internal Docker error");
        Self::source(ErrorKind::Internal, err)
    }
}

/// Allow some fields to default to their default value if deserializing them fails
/// https://users.rust-lang.org/t/solved-serde-deserialization-on-error-use-default-values/6681/2
fn ok_or_default<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: DeserializeOwned + Default,
    D: Deserializer<'de>,
{
    // Convert to `Value` first so that we capture the whole input in case it fails later on the specific type
    let v: serde_json::Value = Deserialize::deserialize(deserializer)?;
    Ok(T::deserialize(v).unwrap_or_default())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Project {
    Creating(ProjectCreating),
    Attaching(ProjectAttaching),
    Recreating(ProjectRecreating),
    Starting(ProjectStarting),
    Restarting(ProjectRestarting),
    Started(ProjectStarted),
    Ready(ProjectReady),
    Rebooting(ProjectRebooting),
    Stopping(ProjectStopping),
    Stopped(ProjectStopped),
    Destroying(ProjectDestroying),
    Destroyed(ProjectDestroyed),
    Errored(ProjectError),
    Deleted,
}

impl_from_variant!(Project:
                   ProjectCreating => Creating,
                   ProjectAttaching => Attaching,
                   ProjectRecreating => Recreating,
                   ProjectStarting => Starting,
                   ProjectRestarting => Restarting,
                   ProjectStarted => Started,
                   ProjectReady => Ready,
                   ProjectStopping => Stopping,
                   ProjectStopped => Stopped,
                   ProjectRebooting => Rebooting,
                   ProjectDestroying => Destroying,
                   ProjectDestroyed => Destroyed,
                   ProjectError => Errored);

impl Project {
    pub fn stop(self) -> Result<Self, Error> {
        if let Some(container) = self.container() {
            Ok(Self::Stopping(ProjectStopping { container }))
        } else {
            Err(Error::custom(
                ErrorKind::InvalidOperation,
                format!("cannot stop a project in the `{}` state", self.state()),
            ))
        }
    }

    pub fn reboot(self) -> Result<Self, Error> {
        if let Some(container) = self.container() {
            Ok(Self::Rebooting(ProjectRebooting { container }))
        } else {
            Err(Error::custom(
                ErrorKind::InvalidOperation,
                format!("cannot reboot a project in the `{}` state", self.state()),
            ))
        }
    }

    pub fn destroy(self) -> Result<Self, Error> {
        if let Some(container) = self.container() {
            Ok(Self::Destroying(ProjectDestroying { container }))
        } else {
            Ok(Self::Destroyed(ProjectDestroyed { destroyed: None }))
        }
    }

    pub async fn delete<Ctx>(self, ctx: &Ctx) -> Result<(), Error>
    where
        Ctx: DockerContext,
    {
        let Some(ref id) = self.container_id() else {
            error!("No container id found for deleting");
            return Err(Error::custom(
                ErrorKind::ProjectUnavailable,
                "container id not found",
            ));
        };
        ctx.docker()
            .remove_container(
                id,
                Some(RemoveContainerOptions {
                    v: true, // This only deletes anonymous volumes (there should be none)
                    force: true,
                    ..Default::default()
                }),
            )
            .await?;

        let Some(mounts) = self
            .container()
            .and_then(|c| c.host_config)
            .and_then(|hc| hc.mounts)
        else {
            warn!("No mounts found when deleting container {}", id);
            return Ok(());
        };
        // Delete the volume linked to this container.
        // This, along with the DB row removal is what separates a
        // project stop (destroy) from a project delete.
        for mount in mounts {
            if mount.typ.is_some_and(|t| t == MountTypeEnum::VOLUME) && mount.source.is_some() {
                let name = mount.source.unwrap();
                ctx.docker()
                    .remove_volume(name.as_str(), Some(RemoveVolumeOptions { force: true }))
                    .await?;
                info!("deleted volume {name}");
            }
        }

        Ok(())
    }

    pub fn start(self) -> Result<Self, Error> {
        if let Some(container) = self.container() {
            Ok(Self::Starting(ProjectStarting {
                container,
                restart_count: 0,
            }))
        } else {
            Err(Error::custom(
                ErrorKind::InvalidOperation,
                format!("cannot start a project in the `{}` state", self.state()),
            ))
        }
    }

    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }

    pub fn is_destroyed(&self) -> bool {
        matches!(self, Self::Destroyed(_))
    }

    pub fn is_stopped(&self) -> bool {
        matches!(self, Self::Stopped(_))
    }

    pub fn target_ip(&self) -> Result<Option<IpAddr>, Error> {
        match self.clone() {
            Self::Ready(project_ready) => Ok(Some(*project_ready.target_ip())),
            _ => Ok(None), // not ready
        }
    }

    pub fn target_addr(&self) -> Result<Option<SocketAddr>, Error> {
        Ok(self
            .target_ip()?
            .map(|target_ip| SocketAddr::new(target_ip, RUNTIME_API_PORT)))
    }

    pub fn state(&self) -> String {
        match self {
            Self::Started(_) => "started".to_string(),
            Self::Ready(_) => "ready".to_string(),
            Self::Stopped(_) => "stopped".to_string(),
            Self::Starting(ProjectStarting { restart_count, .. }) => {
                if *restart_count > 0 {
                    format!("starting (attempt {restart_count})")
                } else {
                    "starting".to_string()
                }
            }
            Self::Recreating(ProjectRecreating { recreate_count, .. }) => {
                format!("recreating (attempt {recreate_count})")
            }
            Self::Restarting(ProjectRestarting { restart_count, .. }) => {
                format!("restarting (attempt {restart_count})")
            }
            Self::Stopping(_) => "stopping".to_string(),
            Self::Rebooting(_) => "rebooting".to_string(),
            Self::Creating(ProjectCreating { recreate_count, .. }) => {
                if *recreate_count > 0 {
                    format!("creating (attempt {recreate_count})")
                } else {
                    "creating".to_string()
                }
            }
            Self::Attaching(ProjectAttaching { recreate_count, .. }) => {
                if *recreate_count > 0 {
                    format!("attaching (attempt {recreate_count})")
                } else {
                    "attaching".to_string()
                }
            }
            Self::Destroying(_) => "destroying".to_string(),
            Self::Destroyed(_) => "destroyed".to_string(),
            Self::Errored(_) => "error".to_string(),
            Self::Deleted => "deleted".to_string(),
        }
    }

    pub fn container(&self) -> Option<ContainerInspectResponse> {
        match self {
            Self::Starting(ProjectStarting { container, .. })
            | Self::Started(ProjectStarted { container, .. })
            | Self::Recreating(ProjectRecreating { container, .. })
            | Self::Restarting(ProjectRestarting { container, .. })
            | Self::Attaching(ProjectAttaching { container, .. })
            | Self::Ready(ProjectReady { container, .. })
            | Self::Stopping(ProjectStopping { container, .. })
            | Self::Stopped(ProjectStopped { container, .. })
            | Self::Rebooting(ProjectRebooting { container, .. })
            | Self::Destroying(ProjectDestroying { container })
            | Self::Destroyed(ProjectDestroyed {
                destroyed: Some(container),
            }) => Some(container.clone()),
            Self::Errored(ProjectError { ctx: Some(ctx), .. }) => ctx.container(),
            Self::Errored(_) | Self::Creating(_) | Self::Destroyed(_) | Self::Deleted => None,
        }
    }

    pub fn initial_key(&self) -> Option<&str> {
        if let Self::Creating(creating) = self {
            Some(creating.initial_key())
        } else {
            None
        }
    }

    pub fn container_id(&self) -> Option<String> {
        self.container().and_then(|container| container.id)
    }

    pub fn idle_minutes(&self) -> Option<u64> {
        self.container().map(|container| container.idle_minutes())
    }
}

impl From<Project> for shuttle_common::models::project::State {
    fn from(project: Project) -> Self {
        match project {
            Project::Creating(ProjectCreating { recreate_count, .. }) => {
                Self::Creating { recreate_count }
            }
            Project::Attaching(ProjectAttaching { recreate_count, .. }) => {
                Self::Attaching { recreate_count }
            }
            Project::Recreating(ProjectRecreating { recreate_count, .. }) => {
                Self::Recreating { recreate_count }
            }
            Project::Starting(ProjectStarting { restart_count, .. }) => {
                Self::Starting { restart_count }
            }
            Project::Restarting(ProjectRestarting { restart_count, .. }) => {
                Self::Restarting { restart_count }
            }
            Project::Started(_) => Self::Started,
            Project::Ready(_) => Self::Ready,
            Project::Stopping(_) => Self::Stopping,
            Project::Stopped(_) => Self::Stopped,
            Project::Rebooting(_) => Self::Rebooting,
            Project::Destroying(_) => Self::Destroying,
            Project::Destroyed(_) => Self::Destroyed,
            Project::Errored(ProjectError { message, .. }) => Self::Errored { message },
            Project::Deleted => Self::Deleted,
        }
    }
}

#[async_trait]
impl<Ctx> State<Ctx> for Project
where
    Ctx: DockerContext,
{
    type Next = Self;
    type Error = Infallible;

    #[instrument("getting next project state", skip_all, fields(state = %self.state()))]
    async fn next(self, ctx: &Ctx) -> Result<Self::Next, Self::Error> {
        let previous = self.clone();
        let previous_state = previous.state();

        let mut new = match self {
            Self::Creating(creating) => creating.next(ctx).await.into_try_state(),
            Self::Attaching(attaching) => match attaching.clone().next(ctx).await {
                Err(ProjectError {
                    kind: ProjectErrorKind::NoNetwork,
                    ..
                }) => {
                    // Recreate the container to try and connect to the network again
                    Ok(Self::Recreating(ProjectRecreating {
                        container: attaching.container,
                        recreate_count: attaching.recreate_count,
                    }))
                }
                attaching => attaching.into_try_state(),
            },
            Self::Recreating(recreating) => recreating.next(ctx).await.into_try_state(),
            Self::Starting(starting) => match starting.clone().next(ctx).await {
                Err(error) => {
                    error!(
                        error = &error as &dyn std::error::Error,
                        "project failed to start. Will restart it"
                    );

                    Ok(Self::Restarting(ProjectRestarting {
                        container: starting.container,
                        restart_count: starting.restart_count,
                    }))
                }
                starting => starting.into_try_state(),
            },
            Self::Restarting(restarting) => restarting.next(ctx).await.into_try_state(),
            Self::Started(started) => match started.next(ctx).await {
                Ok(ProjectReadying::Ready(ready)) => Ok(ready.into()),
                Ok(ProjectReadying::Started(started)) => Ok(started.into()),
                Err(err) => Ok(Self::Errored(err)),
            },
            Self::Ready(ready) => match ready.next(ctx).await {
                Ok(ProjectRunning::Ready(ready)) => Ok(ready.into()),
                Ok(ProjectRunning::Idle(stopping)) => Ok(stopping.into()),
                Err(err) => Ok(Self::Errored(err)),
            },
            Self::Stopped(stopped) => stopped.next(ctx).await.into_try_state(),
            Self::Stopping(stopping) => stopping.next(ctx).await.into_try_state(),
            Self::Rebooting(rebooting) => rebooting.next(ctx).await.into_try_state(),
            Self::Destroying(destroying) => destroying.next(ctx).await.into_try_state(),
            Self::Destroyed(destroyed) => destroyed.next(ctx).await.into_try_state(),
            Self::Errored(errored) => Ok(Self::Errored(errored)),
            Self::Deleted => Ok(Self::Deleted),
        };

        if let Ok(Self::Errored(errored)) = &mut new {
            errored.ctx = Some(Box::new(previous));
            error!(error = ?errored, "state for project errored");
        }

        let new_state = new.as_ref().unwrap().state();
        let container_id = new
            .as_ref()
            .unwrap()
            .container_id()
            .map(|id| format!("{id}: "))
            .unwrap_or_default();
        debug!("{container_id}{previous_state} -> {new_state}");

        new
    }
}

impl TryState for Project {
    type ErrorVariant = ProjectError;

    fn into_result(self) -> Result<Self, Self::ErrorVariant> {
        match self {
            Self::Errored(perr) => Err(perr),
            otherwise => Ok(otherwise),
        }
    }
}

#[async_trait]
impl<Ctx> Refresh<Ctx> for Project
where
    Ctx: DockerContext,
{
    type Error = Error;

    async fn refresh(self, ctx: &Ctx) -> Result<Self, Self::Error> {
        let refreshed = match self {
            Self::Creating(creating) => Self::Creating(creating),
            Self::Attaching(attaching) => Self::Attaching(attaching),
            Self::Starting(ProjectStarting {
                container,
                restart_count,
            }) => match container.clone().refresh(ctx).await {
                Ok(container) => match safe_unwrap!(container.state.status) {
                    ContainerStateStatusEnum::RUNNING => Self::Started(ProjectStarted::new(
                        container,
                        restart_count,
                        VecDeque::new(),
                    )),
                    // We can reach the starting state because either:
                    // - The project was created for the first time
                    // - It crashed and we are restarting it
                    ContainerStateStatusEnum::CREATED | ContainerStateStatusEnum::EXITED => {
                        Self::Starting(ProjectStarting {
                            container,
                            restart_count,
                        })
                    }
                    _ => {
                        warn!("container resource has drifted out of sync from the starting state: cannot recover");
                        Self::Restarting(ProjectRestarting {
                            container,
                            restart_count,
                        })
                    }
                },
                Err(DockerError::DockerResponseServerError {
                    status_code: 404, ..
                }) => {
                    // container not found, let's try to recreate it
                    // with the same image
                    Self::Creating(ProjectCreating::from_container(container, 0)?)
                }
                Err(err) => return Err(err.into()),
            },
            Self::Started(ProjectStarted {
                container,
                stats,
                start_count,
                ..
            }) => match container.clone().refresh(ctx).await {
                Ok(container) => match safe_unwrap!(container.state.status) {
                    ContainerStateStatusEnum::RUNNING => {
                        Self::Started(ProjectStarted::new(container, start_count, stats))
                    }
                    // Restart the container if it went down
                    ContainerStateStatusEnum::EXITED => Self::Restarting(ProjectRestarting {
                        container,
                        restart_count: start_count + 1,
                    }),
                    _ => {
                        warn!("container resource has drifted out of sync from a started state. Will now reboot it");
                        Self::Restarting(ProjectRestarting {
                            container,
                            restart_count: start_count + 1,
                        })
                    }
                },
                Err(DockerError::DockerResponseServerError {
                    status_code: 404, ..
                }) => {
                    // container not found, let's try to recreate it
                    // with the same image
                    Self::Creating(ProjectCreating::from_container(container, 0)?)
                }
                Err(err) => return Err(err.into()),
            },
            Self::Ready(ProjectReady {
                container,
                service,
                stats,
                ..
            }) => match container.clone().refresh(ctx).await {
                Ok(container) => match safe_unwrap!(container.state.status) {
                    ContainerStateStatusEnum::RUNNING => {
                        Self::Ready(ProjectReady::new(container, service, stats))
                    }
                    // Restart the container if it went down
                    ContainerStateStatusEnum::EXITED => Self::Restarting(ProjectRestarting {
                        container,
                        restart_count: 0,
                    }),
                    _ => {
                        warn!("container resource has drifted out of sync from a ready state. Will now reboot it");
                        Self::Restarting(ProjectRestarting {
                            container,
                            restart_count: 0,
                        })
                    }
                },
                Err(DockerError::DockerResponseServerError {
                    status_code: 404, ..
                }) => {
                    // container not found, let's try to recreate it
                    // with the same image
                    Self::Creating(ProjectCreating::from_container(container, 0)?)
                }
                Err(err) => return Err(err.into()),
            },
            Self::Stopping(ProjectStopping { container }) => match container
                .clone()
                .refresh(ctx)
                .await
            {
                Ok(container) => match safe_unwrap!(container.state.status) {
                    ContainerStateStatusEnum::RUNNING => {
                        Self::Stopping(ProjectStopping { container })
                    }
                    ContainerStateStatusEnum::EXITED => Self::Stopped(ProjectStopped { container }),
                    _ => {
                        warn!("container resource has drifted out of sync from a stopping state");
                        Self::Stopping(ProjectStopping { container })
                    }
                },
                Err(err) => return Err(err.into()),
            },
            Self::Restarting(restarting) => Self::Restarting(restarting),
            Self::Recreating(recreating) => Self::Recreating(recreating),
            Self::Stopped(stopped) => Self::Stopped(stopped),
            Self::Rebooting(rebooting) => Self::Rebooting(rebooting),
            Self::Destroying(destroying) => Self::Destroying(destroying),
            Self::Destroyed(destroyed) => Self::Destroyed(destroyed),
            Self::Errored(err) => Self::Errored(err),
            Self::Deleted => Self::Deleted,
        };
        Ok(refreshed)
    }
}

pub async fn refresh_with_retry(
    project: Project,
    ctx: &impl DockerContext,
) -> Result<Project, Error> {
    let max_attempt = 3;
    let mut num_attempt = 1;
    let mut proj = Box::new(project);

    loop {
        let refreshed = proj.refresh(ctx).await;
        match refreshed.as_ref() {
            Ok(Project::Errored(err)) => match &err.ctx {
                Some(err_ctx) => {
                    if num_attempt >= max_attempt {
                        return refreshed;
                    } else {
                        num_attempt += 1;
                        proj = err_ctx.clone();
                        tokio::time::sleep(Duration::from_millis(100_u64 * 2_u64.pow(num_attempt)))
                            .await
                    }
                }
                _ => return refreshed,
            },
            _ => return refreshed,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectCreating {
    project_name: ProjectName,
    /// The project id which this deployer is created for
    project_id: Ulid,
    /// The admin secret with which the start deployer
    initial_key: String,
    /// Override the default fqdn (`${project_name}.${public}`)
    fqdn: Option<String>,
    /// Override the default image (specified in the args to this gateway)
    image: Option<String>,
    /// Configuration will be extracted from there if specified (will
    /// take precedence over other overrides)
    from: Option<ContainerInspectResponse>,
    // Use default for backward compatibility. Can be removed when all projects in the DB have this property set
    #[serde(default)]
    recreate_count: usize,
    /// Label set on container as to how many minutes to wait before a project is considered idle
    #[serde(default = "default_idle_minutes")]
    idle_minutes: u64,
}

impl ProjectCreating {
    pub fn new(
        project_name: ProjectName,
        project_id: Ulid,
        initial_key: String,
        idle_minutes: u64,
    ) -> Self {
        Self {
            project_name,
            project_id,
            initial_key,
            fqdn: None,
            image: None,
            from: None,
            recreate_count: 0,
            idle_minutes,
        }
    }

    pub fn from_container(
        container: ContainerInspectResponse,
        recreate_count: usize,
    ) -> Result<Self, ProjectError> {
        let project_name = container.project_name()?;
        let project_id = container.project_id()?;
        let idle_minutes = container.idle_minutes();
        let initial_key = container.initial_key()?;

        Ok(Self {
            project_name,
            project_id,
            initial_key,
            fqdn: None,
            image: None,
            from: Some(container),
            recreate_count,
            idle_minutes,
        })
    }

    pub fn from(mut self, from: ContainerInspectResponse) -> Self {
        self.from = Some(from);
        self
    }

    pub fn with_fqdn(mut self, fqdn: String) -> Self {
        self.fqdn = Some(fqdn);
        self
    }

    pub fn new_with_random_initial_key(
        project_name: ProjectName,
        project_id: Ulid,
        idle_minutes: u64,
    ) -> Self {
        let initial_key = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);
        Self::new(project_name, project_id, initial_key, idle_minutes)
    }

    pub fn with_image(mut self, image: String) -> Self {
        self.image = Some(image);
        self
    }

    pub fn project_name(&self) -> &ProjectName {
        &self.project_name
    }

    pub fn initial_key(&self) -> &str {
        &self.initial_key
    }

    pub fn fqdn(&self) -> &Option<String> {
        &self.fqdn
    }

    fn container_name<C: DockerContext>(&self, ctx: &C) -> String {
        let prefix = &ctx.container_settings().prefix;

        let Self { project_name, .. } = &self;

        format!("{prefix}{project_name}_run")
    }

    fn volume_name<C: DockerContext>(&self, ctx: &C) -> String {
        let prefix = &ctx.container_settings().prefix;

        let Self { project_name, .. } = &self;

        format!("{prefix}{project_name}_vol")
    }

    fn generate_container_config<C: DockerContext>(
        &self,
        ctx: &C,
    ) -> (CreateContainerOptions<String>, Config<String>) {
        let ContainerSettings {
            image: default_image,
            prefix,
            provisioner_host,
            builder_host,
            auth_uri,
            resource_recorder_uri,
            fqdn: public,
            extra_hosts,
            ..
        } = ctx.container_settings();

        let Self {
            initial_key,
            project_name,
            fqdn,
            image,
            idle_minutes,
            ..
        } = &self;

        let create_container_options = CreateContainerOptions {
            name: self.container_name(ctx),
            platform: None,
        };

        let container_config = self
            .from
            .as_ref()
            .and_then(|container| container.config.clone())
            .unwrap_or_else(|| {
                deserialize_json!({
                    "Image": image.as_ref().unwrap_or(default_image),
                    "Hostname": format!("{prefix}{project_name}"),
                    "Labels": {
                        "shuttle.prefix": prefix,
                        "shuttle.project": project_name,
                        "shuttle.project_id": self.project_id.to_string(),
                        "shuttle.idle_minutes": format!("{idle_minutes}"),
                    },
                    "Cmd": [
                        "--admin-secret",
                        initial_key,
                        "--project",
                        project_name,
                        "--api-address",
                        format!("0.0.0.0:{RUNTIME_API_PORT}"),
                        "--provisioner-address",
                        format!("http://{provisioner_host}:8000"),
                        "--proxy-address",
                        "0.0.0.0:8000",
                        "--proxy-fqdn",
                        fqdn.clone().unwrap_or(format!("{project_name}.{public}")),
                        "--artifacts-path",
                        "/opt/shuttle",
                        "--state",
                        "/opt/shuttle/deployer.sqlite",
                        "--auth-uri",
                        auth_uri,
                        "--builder-uri",
                        format!("http://{builder_host}:8000"),
                        "--resource-recorder",
                        resource_recorder_uri,
                        "--project-id",
                        self.project_id.to_string()
                    ],
                })
            });

        let mut config = Config::<String>::from(container_config);

        config.host_config = deserialize_json!({
            "Mounts": [{
                "Target": "/opt/shuttle",
                "Source": self.volume_name(ctx),
                "Type": "volume"
            }],
            // https://docs.docker.com/config/containers/resource_constraints/#memory
            "Memory": 6442450000i64, // 6 GiB hard limit
            "MemoryReservation": 4295000000i64, // 4 GiB soft limit, applied if host is low on memory
            // https://docs.docker.com/config/containers/resource_constraints/#cpu
            "CpuPeriod": 100000i64,
            "CpuQuota": 400000i64,
            "ExtraHosts": extra_hosts,
        });

        debug!(
            r"generated a container configuration:
CreateContainerOpts: {create_container_options:#?}
Config: {config:#?}
"
        );

        (create_container_options, config)
    }
}

#[async_trait]
impl<Ctx> State<Ctx> for ProjectCreating
where
    Ctx: DockerContext,
{
    type Next = ProjectAttaching;
    type Error = ProjectError;

    #[instrument(name = "try to create container", skip_all)]
    async fn next(self, ctx: &Ctx) -> Result<Self::Next, Self::Error> {
        let container_name = self.container_name(ctx);
        let Self { recreate_count, .. } = self;

        let container = ctx
            .docker()
            // If container already exists, use that
            .inspect_container(&container_name.clone(), None)
            // Otherwise create it
            .or_else(|err| async move {
                if matches!(err, DockerError::DockerResponseServerError { status_code, .. } if status_code == 404) {
                    let (opts, config) = self.generate_container_config(ctx);
                    ctx.docker()
                        .create_container(Some(opts), config)
                        .and_then(|_| ctx.docker().inspect_container(&container_name, None))
                        .await
                } else {
                    Err(err)
                }
            })
            .await?;
        Ok(ProjectAttaching {
            container,
            recreate_count,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectAttaching {
    container: ContainerInspectResponse,
    // Use default for backward compatibility. Can be removed when all projects in the DB have this property set
    #[serde(default)]
    recreate_count: usize,
}

#[async_trait]
impl<Ctx> State<Ctx> for ProjectAttaching
where
    Ctx: DockerContext,
{
    type Next = ProjectStarting;
    type Error = ProjectError;

    #[instrument(name = "try to attach container to network", skip_all)]
    async fn next(self, ctx: &Ctx) -> Result<Self::Next, Self::Error> {
        let Self { container, .. } = self;

        let container_id = safe_unwrap!(container.id);
        let ContainerSettings { network_name, .. } = ctx.container_settings();

        // Disconnect the bridge network before trying to start up
        // For docker bug https://github.com/docker/cli/issues/1891
        //
        // Also disconnecting from all network because docker just losses track of their IDs sometimes when restarting
        for network in safe_unwrap!(container.network_settings.networks).keys() {
            ctx.docker().disconnect_network(network, DisconnectNetworkOptions{
            container: container_id,
            force: true,
        })
            .await
            .or_else(|err| {
                if matches!(err, DockerError::DockerResponseServerError { status_code, .. } if status_code == 500) {
                    info!("already disconnected from the {network} network");
                    Ok(())
                } else {
                    Err(err)
                }
            })?;
        }

        // Make sure the container is connected to the user network
        let network_config = ConnectNetworkOptions {
            container: container_id,
            endpoint_config: Default::default(),
        };
        ctx.docker()
            .connect_network(network_name, network_config)
            .await
            .or_else(|err| {
                if matches!(
                    err,
                    DockerError::DockerResponseServerError { status_code, .. } if status_code == 409
                ) {
                    info!("already connected to the shuttle network");
                    Ok(())
                } else {
                    error!(
                        error = &err as &dyn std::error::Error,
                        "failed to connect to shuttle network"
                    );
                    Err(ProjectError::no_network(
                        "failed to connect to shuttle network",
                    ))
                }
            })?;

        let container = container.refresh(ctx).await?;

        Ok(ProjectStarting {
            container,
            restart_count: 0,
        })
    }
}

// Special state to try and recreate a container if it failed to be created
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectRecreating {
    container: ContainerInspectResponse,
    recreate_count: usize,
}

#[async_trait]
impl<Ctx> State<Ctx> for ProjectRecreating
where
    Ctx: DockerContext,
{
    type Next = ProjectCreating;
    type Error = ProjectError;

    #[instrument(name = "recreating container", skip_all, fields(recreate_count = %self.recreate_count))]
    async fn next(self, ctx: &Ctx) -> Result<Self::Next, Self::Error> {
        let Self {
            container,
            recreate_count,
        } = self;
        let container_id = safe_unwrap!(container.id);

        ctx.docker()
            .stop_container(container_id, Some(StopContainerOptions { t: 1 }))
            .await
            .unwrap_or(());
        ctx.docker()
            .remove_container(
                container_id,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap_or(());

        if recreate_count < MAX_RECREATES {
            sleep(Duration::from_secs(5)).await;
            Ok(ProjectCreating::from_container(
                container,
                recreate_count + 1,
            )?)
        } else {
            Err(ProjectError::internal("too many recreates"))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectStarting {
    container: ContainerInspectResponse,
    // Use default for backward compatibility. Can be removed when all projects in the DB have this property set
    #[serde(default)]
    restart_count: usize,
}

#[async_trait]
impl<Ctx> State<Ctx> for ProjectStarting
where
    Ctx: DockerContext,
{
    type Next = ProjectStarted;
    type Error = ProjectError;

    #[instrument(name = "try to start container", skip_all)]
    async fn next(self, ctx: &Ctx) -> Result<Self::Next, Self::Error> {
        let Self {
            container,
            restart_count,
        } = self;
        let container_id = safe_unwrap!(container.id);

        ctx.docker()
            .start_container::<String>(container_id, None)
            .await
            .or_else(|err| {
                if matches!(err, DockerError::DockerResponseServerError { status_code, .. } if status_code == 304) {
                    // Already started
                    Ok(())
                } else {
                    Err(err)
                }
            })?;

        let container = container.refresh(ctx).await?;

        Ok(Self::Next::new(container, restart_count, VecDeque::new()))
    }
}

/// Special state for when `ProjectStarting` fails to retry it
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectRestarting {
    container: ContainerInspectResponse,
    restart_count: usize,
}

impl ProjectRestarting {
    /// Has the restart count been exhausted
    pub fn exhausted(&self) -> bool {
        Self::reached_max_restarts(self.restart_count)
    }

    fn reached_max_restarts(restart_count: usize) -> bool {
        restart_count >= MAX_RESTARTS
    }
}

#[async_trait]
impl<Ctx> State<Ctx> for ProjectRestarting
where
    Ctx: DockerContext,
{
    type Next = ProjectStarting;
    type Error = ProjectError;

    #[instrument(name = "restarting container", skip_all, fields(restart_count = %self.restart_count))]
    async fn next(self, ctx: &Ctx) -> Result<Self::Next, Self::Error> {
        let Self {
            container,
            restart_count,
        } = self;

        let container_id = safe_unwrap!(container.id);

        // Stop it just to be safe
        ctx.docker()
            .stop_container(container_id, Some(StopContainerOptions { t: 1 }))
            .await
            .unwrap_or(());

        debug!("project restarted {} times", restart_count);

        if !Self::reached_max_restarts(restart_count) {
            sleep(Duration::from_secs(5)).await;
            Ok(ProjectStarting {
                container,
                restart_count: restart_count + 1,
            })
        } else {
            Err(ProjectError::internal("too many restarts"))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectStarted {
    container: ContainerInspectResponse,
    service: Option<Service>,
    // Use default for backward compatibility. Can be removed when all projects in the DB have this property set
    #[serde(default)]
    start_count: usize,
    // Use default for backward compatibility. Can be removed when all projects in the DB have this property set
    #[serde(default, deserialize_with = "ok_or_default")]
    stats: VecDeque<u64>,
}

impl ProjectStarted {
    pub fn new(
        container: ContainerInspectResponse,
        start_count: usize,
        stats: VecDeque<u64>,
    ) -> Self {
        Self {
            container,
            service: None,
            start_count,
            stats,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ProjectReadying {
    Ready(ProjectReady),
    Started(ProjectStarted),
}

#[async_trait]
impl<Ctx> State<Ctx> for ProjectStarted
where
    Ctx: DockerContext,
{
    type Next = ProjectReadying;
    type Error = ProjectError;

    #[instrument(name = "wait for container to be ready", skip_all)]
    async fn next(self, ctx: &Ctx) -> Result<Self::Next, Self::Error> {
        let Self {
            container,
            service,
            start_count,
            stats,
        } = self;
        let container = container.refresh(ctx).await?;
        let mut service = match service {
            Some(service) => service,
            None => Service::from_container(container.clone())?,
        };

        if service.is_healthy().await {
            Ok(Self::Next::Ready(ProjectReady {
                container,
                service,
                stats,
            }))
        } else {
            let started_at =
                chrono::DateTime::parse_from_rfc3339(safe_unwrap!(container.state.started_at))
                    .map_err(|_err| {
                        ProjectError::internal("invalid `started_at` response from Docker daemon")
                    })?;
            let now = chrono::offset::Utc::now();
            if started_at + chrono::Duration::seconds(120) < now {
                return Err(ProjectError::internal(
                    "project did not become healthy in time",
                ));
            }

            Ok(Self::Next::Started(ProjectStarted {
                container,
                service: Some(service),
                start_count,
                stats,
            }))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectReady {
    container: ContainerInspectResponse,
    service: Service,
    // Use default for backward compatibility. Can be removed when all projects in the DB have this property set
    #[serde(default, deserialize_with = "ok_or_default")]
    stats: VecDeque<u64>,
}

impl ProjectReady {
    pub fn new(
        container: ContainerInspectResponse,
        service: Service,
        stats: VecDeque<u64>,
    ) -> Self {
        Self {
            container,
            service,
            stats,
        }
    }
}

#[instrument(name = "getting container stats from the cgroup file", skip_all)]
async fn get_container_stats(container: &ContainerInspectResponse) -> Result<u64, ProjectError> {
    let id = safe_unwrap!(container.id);

    let usage: u64 =
        std::fs::read_to_string(format!("/sys/fs/cgroup/cpuacct/docker/{id}/cpuacct.usage"))
            .unwrap_or_default()
            .parse()
            .unwrap_or_default();
    // TODO: the above solution only works for cgroup v1
    // This is the version used by our server. However on my local nix setup I have cgroup v2 and had to use the
    // following to get the 'usage_usec' which is on the first line
    // let usage: u64 = std::fs::read_to_string(format!(
    //     "/sys/fs/cgroup/system.slice/docker-{id}.scope/cpu.stat"
    // ))
    // .unwrap_or_default()
    // .lines()
    // .next()
    // .unwrap()
    // .split(' ')
    // .nth(1)
    // .unwrap_or_default()
    // .parse::<u64>()
    // .unwrap_or_default()
    //     * 1_000;

    Ok(usage)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ProjectRunning {
    Ready(ProjectReady),
    Idle(ProjectStopping),
}

#[async_trait]
impl<Ctx> State<Ctx> for ProjectReady
where
    Ctx: DockerContext,
{
    type Next = ProjectRunning;
    type Error = ProjectError;

    #[instrument(name = "check if container is still healthy", skip_all)]
    async fn next(mut self, _ctx: &Ctx) -> Result<Self::Next, Self::Error> {
        let Self {
            container,
            mut service,
            mut stats,
        } = self;

        if !service.is_healthy().await {
            return Err(ProjectError::internal(
                "running project is no longer healthy",
            ));
        }
        let idle_minutes = container.idle_minutes();

        // Idle minutes of `0` means it is disabled and the project will always stay up
        if idle_minutes < 1 {
            return Ok(Self::Next::Ready(ProjectReady {
                container,
                service,
                stats,
            }));
        }

        let new_stat = get_container_stats(&container).await?;

        stats.push_back(new_stat);

        let mut last = None;

        while stats.len() > idle_minutes as usize {
            last = stats.pop_front();
        }

        let Some(last) = last else {
            return Ok(Self::Next::Ready(ProjectReady {
                container,
                service,
                stats,
            }));
        };

        let cpu_per_minute = (new_stat - last) / idle_minutes;

        debug!(
            "{} has {} CPU usage per minute",
            service.name, cpu_per_minute
        );

        // From analysis we know the following kind of CPU usage for different kinds of idle projects
        // Web framework uses 6_200_000 CPU per minute
        // Serenity uses 20_000_000 CPU per minute
        //
        // We want to make sure we are able to stop these kinds of projects
        //
        // Now, the following kind of CPU usage has been observed for different kinds of projects having
        // 2 web requests / processing 2 discord messages per minute
        // Web framework uses 100_000_000 CPU per minute
        // Serenity uses 30_000_000 CPU per minute
        //
        // And projects at these levels we will want to keep active. However, the 30_000_000
        // for an "active" discord will be to close to the 20_000_000 of an idle framework. And
        // discord will have more traffic in anyway. So using the 100_000_000 threshold of an
        // active framework for now
        if cpu_per_minute < 100_000_000 {
            Ok(Self::Next::Idle(ProjectStopping { container }))
        } else {
            Ok(Self::Next::Ready(ProjectReady {
                container,
                service,
                stats,
            }))
        }
    }
}

impl ProjectReady {
    pub fn name(&self) -> &ProjectName {
        &self.service.name
    }

    pub fn target_ip(&self) -> &IpAddr {
        &self.service.target
    }

    pub async fn start_last_deploy(&mut self, jwt: String, admin_secret: String) {
        if let Err(error) = self.service.start_last_deploy(jwt, admin_secret).await {
            error!(error, "failed to start last running deploy");
        };
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthCheckRecord {
    at: chrono::DateTime<chrono::Utc>,
    is_healthy: bool,
}

impl HealthCheckRecord {
    pub fn new(is_healthy: bool) -> Self {
        Self {
            at: chrono::Utc::now(),
            is_healthy,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Service {
    name: ProjectName,
    target: IpAddr,
    last_check: Option<HealthCheckRecord>,
}

impl Service {
    pub fn from_container(container: ContainerInspectResponse) -> Result<Self, ProjectError> {
        let resource_name = container.project_name()?;

        let network = safe_unwrap!(container.network_settings.networks)
            .values()
            .next()
            .ok_or_else(|| ProjectError::internal("project was not linked to a network"))?;

        let target = safe_unwrap!(network.ip_address)
            .parse()
            .map_err(|_| ProjectError::internal("project did not join the network"))?;

        Ok(Self {
            name: resource_name,
            target,
            last_check: None,
        })
    }

    pub fn uri<S: AsRef<str>>(&self, path: S) -> Result<Uri, ProjectError> {
        format!("http://{}:8001{}", self.target, path.as_ref())
            .parse::<Uri>()
            .map_err(|err| err.into())
    }

    #[instrument(name = "calling status endpoint on container", skip_all, fields(project_name = %self.name))]
    pub async fn is_healthy(&mut self) -> bool {
        let uri = self.uri(format!("/projects/{}/status", self.name)).unwrap();
        let resp = timeout(IS_HEALTHY_TIMEOUT, CLIENT.get(uri)).await;
        let is_healthy = matches!(resp, Ok(Ok(res)) if res.status().is_success());
        self.last_check = Some(HealthCheckRecord::new(is_healthy));
        is_healthy
    }

    pub async fn start_last_deploy(
        &mut self,
        jwt: String,
        admin_secret: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        trace!(jwt, "getting last deploy");

        let running_id = self.get_running_deploy(&jwt, &admin_secret).await?;

        trace!(?running_id, "starting deploy");

        if let Some(running_id) = running_id {
            // Start this deployment
            let uri = self.uri(format!(
                "/projects/{}/deployments/{}",
                self.name, running_id
            ))?;

            let req = Request::builder()
                .method(Method::PUT)
                .uri(uri)
                .header(AUTHORIZATION, format!("Bearer {}", jwt))
                .header(X_SHUTTLE_ACCOUNT_NAME.clone(), "gateway")
                .header(X_SHUTTLE_ADMIN_SECRET.clone(), admin_secret)
                .body(Body::empty())?;

            let _ = timeout(IS_HEALTHY_TIMEOUT, CLIENT.request(req)).await;
        }

        Ok(())
    }

    /// Get the last running deployment
    async fn get_running_deploy(
        &self,
        jwt: &str,
        admin_secret: &str,
    ) -> Result<Option<Uuid>, Box<dyn std::error::Error>> {
        let uri = self.uri(format!("/projects/{}/services/{}", self.name, self.name))?;

        let req = Request::builder()
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {}", jwt))
            .header(X_SHUTTLE_ACCOUNT_NAME.clone(), "gateway")
            .header(X_SHUTTLE_ADMIN_SECRET.clone(), admin_secret)
            .body(Body::empty())?;

        let resp = timeout(IS_HEALTHY_TIMEOUT, CLIENT.request(req)).await??;

        let body = hyper::body::to_bytes(resp.into_body()).await?;

        let service: service::Summary = serde_json::from_slice(&body)?;

        if let Some(deployment) = service.deployment {
            Ok(Some(deployment.id))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectRebooting {
    container: ContainerInspectResponse,
}

#[async_trait]
impl<Ctx> State<Ctx> for ProjectRebooting
where
    Ctx: DockerContext,
{
    type Next = ProjectStarting;

    type Error = ProjectError;

    #[instrument(name = "try to reboot container", skip_all)]
    async fn next(self, ctx: &Ctx) -> Result<Self::Next, Self::Error> {
        let Self { mut container } = self;
        ctx.docker()
            .stop_container(
                safe_unwrap!(container.id),
                Some(StopContainerOptions { t: 30 }),
            )
            .await?;

        container = container.refresh(ctx).await?;
        let since = (chrono::Utc::now() - chrono::Duration::minutes(15))
            .timestamp()
            .to_string();
        let until = chrono::Utc::now().timestamp().to_string();

        // Filter and collect `start` events for this project in the last 15 minutes
        let start_events = ctx
            .docker()
            .events(Some(EventsOptions::<&str> {
                since: Some(since),
                until: Some(until),
                filters: HashMap::from([
                    ("container", vec![safe_unwrap!(container.id).as_str()]),
                    ("event", vec!["start"]),
                ]),
            }))
            .try_collect::<Vec<_>>()
            .await?;

        let start_event_count = start_events.len();
        debug!(
            "project started {} times in the last 15 minutes",
            start_event_count
        );

        // If stopped, and has not restarted too much, try to restart
        if start_event_count < MAX_REBOOTS {
            Ok(ProjectStarting {
                container,
                restart_count: 0,
            })
        } else {
            Err(ProjectError::internal(
                "too many restarts in the last 15 minutes",
            ))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectStopping {
    container: ContainerInspectResponse,
}

#[async_trait]
impl<Ctx> State<Ctx> for ProjectStopping
where
    Ctx: DockerContext,
{
    type Next = ProjectStopped;

    type Error = ProjectError;

    #[instrument(name = "try to stop container", skip_all)]
    async fn next(self, ctx: &Ctx) -> Result<Self::Next, Self::Error> {
        let Self { container } = self;

        // Stopping a docker containers sends a SIGTERM which will stop the tokio runtime that deployer starts up.
        // Killing this runtime causes the deployment to enter the `completed` state and it therefore does not
        // start up again when starting up the project's container. Luckily the kill command allows us to change the
        // signal to prevent this from happening.
        //
        // In some future state when all deployers hadle `SIGTERM` correctly, this can be changed to docker stop
        // safely.
        ctx.docker()
            .kill_container(
                safe_unwrap!(container.id),
                Some(KillContainerOptions { signal: "SIGKILL" }),
            )
            .await?;
        Ok(Self::Next {
            container: container.refresh(ctx).await?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectStopped {
    container: ContainerInspectResponse,
}

#[async_trait]
impl<Ctx> State<Ctx> for ProjectStopped
where
    Ctx: DockerContext,
{
    type Next = ProjectStopped;
    type Error = ProjectError;

    #[instrument(name = "keep container in stopped state", skip_all)]
    async fn next(self, _ctx: &Ctx) -> Result<Self::Next, Self::Error> {
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectDestroying {
    container: ContainerInspectResponse,
}

#[async_trait]
impl<Ctx> State<Ctx> for ProjectDestroying
where
    Ctx: DockerContext,
{
    type Next = ProjectDestroyed;
    type Error = ProjectError;

    #[instrument(name = "try to destroy container", skip_all)]
    async fn next(self, ctx: &Ctx) -> Result<Self::Next, Self::Error> {
        let Self { container } = self;
        let container_id = safe_unwrap!(container.id);
        ctx.docker()
            .stop_container(container_id, Some(StopContainerOptions { t: 1 }))
            .await
            .unwrap_or(());
        ctx.docker()
            .remove_container(
                container_id,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap_or(());
        Ok(Self::Next {
            destroyed: Some(container),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectDestroyed {
    destroyed: Option<ContainerInspectResponse>,
}

#[async_trait]
impl<Ctx> State<Ctx> for ProjectDestroyed
where
    Ctx: DockerContext,
{
    type Next = ProjectDestroyed;
    type Error = ProjectError;

    #[instrument(name = "keep container in destroyed state", skip_all)]
    async fn next(self, _ctx: &Ctx) -> Result<Self::Next, Self::Error> {
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProjectErrorKind {
    Internal,
    NoNetwork,
}

/// A runtime error coming from inside a project
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectError {
    kind: ProjectErrorKind,
    message: String,
    ctx: Option<Box<Project>>,
}

impl ProjectError {
    pub fn internal<S: AsRef<str>>(message: S) -> Self {
        Self {
            kind: ProjectErrorKind::Internal,
            message: message.as_ref().to_string(),
            ctx: None,
        }
    }

    pub fn no_network<S: AsRef<str>>(message: S) -> Self {
        Self {
            kind: ProjectErrorKind::NoNetwork,
            message: message.as_ref().to_string(),
            ctx: None,
        }
    }
}

impl std::fmt::Display for ProjectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ProjectError {}

impl From<DockerError> for ProjectError {
    fn from(err: DockerError) -> Self {
        error!(error = %err, "an internal DockerError had to yield a ProjectError");
        Self {
            kind: ProjectErrorKind::Internal,
            message: format!("{}", err),
            ctx: None,
        }
    }
}

impl From<InvalidUri> for ProjectError {
    fn from(uri: InvalidUri) -> Self {
        error!(%uri, "failed to create a health check URI");

        Self {
            kind: ProjectErrorKind::Internal,
            message: uri.to_string(),
            ctx: None,
        }
    }
}

impl From<hyper::Error> for ProjectError {
    fn from(err: hyper::Error) -> Self {
        error!(error = %err, "failed to check project's health");

        Self {
            kind: ProjectErrorKind::Internal,
            message: err.to_string(),
            ctx: None,
        }
    }
}

impl From<ProjectError> for Error {
    fn from(err: ProjectError) -> Self {
        Self::source(ErrorKind::Internal, err)
    }
}

#[async_trait]
impl<Ctx> State<Ctx> for ProjectError
where
    Ctx: DockerContext,
{
    type Next = Self;
    type Error = Infallible;

    #[instrument(name = "keep the container in errored state", skip_all)]
    async fn next(self, _ctx: &Ctx) -> Result<Self::Next, Self::Error> {
        Ok(self)
    }
}

pub mod exec {

    use std::sync::Arc;

    use bollard::service::ContainerState;
    use tokio::sync::mpsc::Sender;

    use crate::{
        service::GatewayService,
        task::{self, BoxedTask, TaskResult},
    };

    use super::*;

    pub async fn revive(
        gateway: Arc<GatewayService>,
        sender: Sender<BoxedTask>,
    ) -> Result<(), ProjectError> {
        for (project_name, _) in gateway
            .iter_projects()
            .await
            .expect("could not list projects")
        {
            match gateway.find_project(&project_name).await.unwrap().state {
                Project::Errored(ProjectError { ctx: Some(ctx), .. }) => {
                    if let Some(container) = ctx.container() {
                        if let Ok(container) = gateway
                            .context()
                            .docker()
                            .inspect_container(safe_unwrap!(container.id), None)
                            .await
                        {
                            match container.state {
                                Some(ContainerState {
                                    status: Some(ContainerStateStatusEnum::EXITED),
                                    ..
                                }) => {
                                    debug!("{} will be revived", project_name.clone());
                                    _ = gateway
                                        .new_task()
                                        .project(project_name)
                                        .and_then(task::run(|ctx| async move {
                                            TaskResult::Done(Project::Rebooting(ProjectRebooting {
                                                container: ctx.state.container().unwrap(),
                                            }))
                                        }))
                                        .send(&sender)
                                        .await;
                                }
                                Some(ContainerState {
                                    status: Some(ContainerStateStatusEnum::RUNNING),
                                    ..
                                })
                                | Some(ContainerState {
                                    status: Some(ContainerStateStatusEnum::CREATED),
                                    ..
                                }) => {
                                    debug!(
                                    "{} is errored but ready according to docker. So restarting it",
                                    project_name.clone()
                                );
                                    _ = gateway
                                        .new_task()
                                        .project(project_name)
                                        .and_then(task::run(|ctx| async move {
                                            TaskResult::Done(Project::Starting(ProjectStarting {
                                                container: ctx.state.container().unwrap(),
                                                restart_count: 0,
                                            }))
                                        }))
                                        .send(&sender)
                                        .await;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                // Currently nothing should enter the stopped state
                Project::Stopped(ProjectStopped { container }) => {
                    if let Ok(container) = gateway
                        .context()
                        .docker()
                        .inspect_container(safe_unwrap!(container.id), None)
                        .await
                    {
                        if container.state.is_some() {
                            _ = gateway
                                .new_task()
                                .project(project_name)
                                .and_then(task::run(|ctx| async move {
                                    TaskResult::Done(Project::Rebooting(ProjectRebooting {
                                        container: ctx.state.container().unwrap(),
                                    }))
                                }))
                                .send(&sender)
                                .await;
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub async fn idle_cch(
        gateway: Arc<GatewayService>,
        sender: Sender<BoxedTask>,
    ) -> Result<(), ProjectError> {
        for project_name in gateway
            .iter_cch_projects()
            .await
            .expect("could not list cch projects")
        {
            if let Project::Ready(ProjectReady { container, .. }) =
                gateway.find_project(&project_name).await.unwrap().state
            {
                if let Ok(container) = gateway
                    .context()
                    .docker()
                    .inspect_container(safe_unwrap!(container.id), None)
                    .await
                {
                    if container.state.is_some() {
                        _ = gateway
                            .new_task()
                            .project(project_name)
                            .and_then(task::run(|ctx| async move {
                                TaskResult::Done(Project::Stopping(ProjectStopping {
                                    container: ctx.state.container().unwrap(),
                                }))
                            }))
                            .send(&sender)
                            .await;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn destroy(
        gateway: Arc<GatewayService>,
        sender: Sender<BoxedTask>,
    ) -> Result<(), ProjectError> {
        for (project_name, _) in gateway
            .iter_projects()
            .await
            .expect("could not list projects")
        {
            let _ = gateway
                .new_task()
                .project(project_name)
                .and_then(task::destroy())
                .send(&sender)
                .await;
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod tests {

    use bollard::models::ContainerState;
    use bollard::service::NetworkSettings;
    use futures::prelude::*;
    use hyper::{Body, Request, StatusCode};

    use super::*;
    use crate::tests::{assert_matches, assert_stream_matches, World};
    use crate::StateExt;

    #[tokio::test]
    async fn create_start_stop_destroy_project() -> anyhow::Result<()> {
        let world = World::new().await;

        let ctx = world.context();

        let project_started = assert_matches!(
            ctx,
            Project::Creating(ProjectCreating {
                project_name: "my-project-test".parse().unwrap(),
                project_id: Ulid::new(),
                initial_key: "test".to_string(),
                fqdn: None,
                image: None,
                from: None,
                recreate_count: 0,
                idle_minutes: 0,
            }),
            #[assertion = "Container created, attach network"]
            Ok(Project::Attaching(ProjectAttaching {
                container: ContainerInspectResponse {
                    state: Some(ContainerState {
                        status: Some(ContainerStateStatusEnum::CREATED),
                        ..
                    }),
                    network_settings: Some(NetworkSettings {
                        networks: Some(networks),
                        ..
                    }),
                    ..
                },
                recreate_count: 0,
            })) if networks.keys().collect::<Vec<_>>() == vec!["bridge"],
            #[assertion = "Container attached, assigned an `id`"]
            Ok(Project::Starting(ProjectStarting {
                container: ContainerInspectResponse {
                    id: Some(container_id),
                    state: Some(ContainerState {
                        status: Some(ContainerStateStatusEnum::CREATED),
                        ..
                    }),
                    network_settings: Some(NetworkSettings {
                        networks: Some(networks),
                        ..
                    }),
                    ..
                },
                restart_count: 0
            })) if networks.keys().collect::<Vec<_>>() == vec![&ctx.container_settings.network_name],
            #[assertion = "Container started, in a running state"]
            Ok(Project::Started(ProjectStarted {
                container: ContainerInspectResponse {
                    id: Some(id),
                    state: Some(ContainerState {
                        status: Some(ContainerStateStatusEnum::RUNNING),
                        ..
                    }),
                    ..
                },
                ..
            })) if id == container_id,
        );

        let delay = sleep(Duration::from_secs(10));
        futures::pin_mut!(delay);
        let mut project_readying = project_started
            .unwrap()
            .into_stream(&ctx)
            .take_until(delay)
            .try_skip_while(|state| future::ready(Ok(!matches!(state, Project::Ready(_)))));

        let project_ready = assert_stream_matches!(
            project_readying,
            #[assertion = "Container is ready"]
            Ok(Project::Ready(ProjectReady {
                container: ContainerInspectResponse {
                    state: Some(ContainerState {
                        status: Some(ContainerStateStatusEnum::RUNNING),
                        ..
                    }),
                    ..
                },
                ..
            })),
        );

        let target_addr = project_ready
            .as_ref()
            .unwrap()
            .target_addr()
            .unwrap()
            .unwrap();

        let client = World::client(target_addr);

        client
            .request(
                Request::get("/projects/my-project-test/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .map_ok(|resp| assert_eq!(resp.status(), StatusCode::OK))
            .await
            .unwrap();

        let project_stopped = assert_matches!(
            ctx,
            project_ready.unwrap().stop().unwrap(),
            #[assertion = "Container is stopped"]
            Ok(Project::Stopped(ProjectStopped {
                container: ContainerInspectResponse {
                    state: Some(ContainerState {
                        status: Some(ContainerStateStatusEnum::EXITED),
                        ..
                    }),
                    ..
                },
            })),
        );

        assert_matches!(
            ctx,
            project_stopped.unwrap().destroy().unwrap(),
            #[assertion = "Container is destroyed"]
            Ok(Project::Destroyed(ProjectDestroyed { destroyed: _ })),
        )
        .unwrap();

        Ok(())
    }
}
