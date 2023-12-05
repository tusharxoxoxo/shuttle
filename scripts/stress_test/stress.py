#!/usr/bin/env python
import argparse

from collections import Counter
import random
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
import asyncio
from dataclasses import dataclass, field
import subprocess
import enum
import re
from typing import Awaitable, Callable, Self

import httpx

ansi_re = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")


class ProjectState(enum.Enum):
    DoesntExist = "doesnt_exist"
    Ready = "ready"
    Idled = "idled"
    Creating = "creating"
    Attaching = "attaching"
    Starting = "starting"
    Started = "started"
    Running = "running"
    Stopped = "stopped"
    Destroyed = "destroyed"
    Destroying = "destroying"
    Deployed = "deployed"
    # TODO: properly handle error state.
    # https://cdn.discordapp.com/attachments/1173622427071287306/1180592218826878986/g7CtmSt.png?ex=657dfb2f&is=656b862f&hm=905cbce5168a8aa729243a0f9f7dd78999516d951e2a94547bb2b799df93c2a5&
    Errored = "errored"


@dataclass
class ErrorMsg:
    error: str
    message: str


@dataclass
class Project:
    name: str
    last_update: datetime = field(default_factory=datetime.now)
    _state: ProjectState = field(default=ProjectState.DoesntExist)
    error_msg: ErrorMsg | None = None
    expected: ProjectState = field(default=ProjectState.DoesntExist)

    @property
    def state(self):
        return self._state.value

    @state.setter
    def state(self, value):
        self._state = value
        self.last_update = datetime.now()

    def set_error_msg(self, err: ErrorMsg | None) -> Self | None:
        if err:
            self.state = ProjectState.Errored
            self.error_msg = err
            return self


status_re = re.compile(r'[Pp]roject ["\'].*["\'] is (?:already )?(\w*)')


error_re = re.compile(r"Error: (.*)\nMessage: (.*)", re.MULTILINE)


def try_parse_error(output: str) -> ErrorMsg | None:
    res = error_re.search(output)
    if res:
        return ErrorMsg(res.group(1), res.group(2))

    return None


@dataclass
class Runner:
    shuttle_root: Path

    async def run(self, command: str) -> tuple[int | None, str]:
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.shuttle_root,
        )
        stdout, stderr = await proc.communicate()
        # if proc.returncode != 0:
        #     print(f"Error with cmd: {command}")
        #     print(stderr.decode())

        out = stdout
        if proc.returncode != 0:
            out = stderr

        result = ansi_re.sub("", out.decode())
        return proc.returncode, result

    async def shuttle(
        self, project: Project, cmd: str, throw_error: bool = False
    ) -> str:
        cmd = f'cargo run -p cargo-shuttle -- --wd examples/axum/hello-world --name "{project.name}" {cmd}'
        ret, output = await self.run(cmd)
        if ret != 0 and throw_error:
            raise RuntimeError(f"Error with command {cmd}")

        return output

    async def status(self, project: Project, update_state: bool = True) -> Project:
        output = await self.shuttle(project, "project status")
        state = status_re.search(output)
        if not state:
            not_found = re.search("Project not found", output)
            if not_found:
                if update_state:
                    project.state = ProjectState.DoesntExist
                return project

            if project.set_error_msg(try_parse_error(output)):
                return project

            raise RuntimeError(f"Couldn't find state in output: {output}")

        if update_state:
            new_state = ProjectState(state.group(1))
            if not (
                new_state == ProjectState.Ready
                and project.state == ProjectState.Deployed
            ):
                project.state = new_state

        return project

    async def start(
        self, project: Project, update_state: bool = True, idle_minutes: int = 5
    ) -> Project:
        print(f"[.] Starting {project.name}")
        output = await self.shuttle(
            project, f"project start --idle-minutes {idle_minutes}"
        )
        state = status_re.search(output)
        if not state:
            if project.set_error_msg(try_parse_error(output)):
                return project
            raise RuntimeError(f"Couldn't find state in output: {output}")

        if update_state:
            project.state = ProjectState(state.group(1))
        return project

    async def stop(self, project: Project, update_state: bool = True) -> Project:
        output = await self.shuttle(project, "project stop")
        state = status_re.search(output)
        if not state:
            if project.set_error_msg(try_parse_error(output)):
                return project
            raise RuntimeError(f"Couldn't find state in output: {output}")

        if update_state:
            project.state = ProjectState(state.group(1))
        return project

    async def deploy(self, project: Project, update_state: bool = True) -> Project:
        output = await self.shuttle(project, "deploy --ad")
        is_success = re.search(
            r"(Runtime started successf?ully)|(Finished dev)", output
        )
        if not is_success:
            if project.set_error_msg(try_parse_error(output)):
                return project
            raise RuntimeError(f"Couldn't find state in output: {output}")

        if update_state:
            project.state = ProjectState.Deployed
        return project

    async def delete(self, project: Project, update_state: bool = True) -> Project:
        output = await self.shuttle(project, "project delete --no-confirmation")
        if update_state:
            project.state = ProjectState.DoesntExist
        return project


def gen_projects(num: int, start: int = 0) -> list[Project]:
    return [Project(name=f"load-stress-{i}") for i in range(start, start + num)]


def concurrency_jitter(
    base_n: int, jitter: int
) -> Callable[[int, datetime], Awaitable[int]]:
    async def jitter_f(n: int, dt: datetime) -> int:
        return base_n + random.randrange(jitter, jitter + 1)

    return jitter_f


def run_with_concurrency(
    n: int,
    operation,
    projects: list[Project],
    update_concurrency_after_each_loop: Callable[[int, datetime], Awaitable[int]]
    | None = None,
):
    if update_concurrency_after_each_loop is None:
        update_concurrency_after_each_loop = concurrency_jitter(n, 0)

    async def gather_with_concurrency(n, *coros):
        futs = set()
        for coro in coros:
            while len(futs) >= n:
                done, futs = await asyncio.wait(
                    futs, return_when=asyncio.FIRST_COMPLETED
                )
                for res in await asyncio.gather(*done):
                    yield res

            futs.add(asyncio.create_task(coro))
            # old_n = n
            # n = await update_concurrency_after_each_loop(old_n, datetime.now())
            # if n != old_n:
            #     print(f"[.] Updating concurrency to {n}")

        rest = await asyncio.gather(*futs)
        for res in rest:
            yield res

    return gather_with_concurrency(n, *(operation(p) for p in projects))


async def periodic_check(runner, projects: list[Project]):
    await asyncio.sleep(10)
    print("[*] Starting periodic check..")
    last_check = datetime.now() - timedelta(hours=1)
    while True:
        try:
            next_check = last_check + timedelta(seconds=20)
            now = datetime.now()
            if next_check >= now:
                sleep_time = next_check - now
                await asyncio.sleep(sleep_time.seconds)

            print("[*] Refreshing status:", flush=True)
            async for project in run_with_concurrency(20, runner.status, projects):
                # print(f"  [*] {project.name}: {project.state}", flush=True)
                last_check = max(last_check, project.last_update)

            stats = Counter([project.state for project in projects])
            print(f" [*] Stats: {stats}")
        except Exception as exc:
            traceback.print_exception(exc)


async def ainput(string: str) -> str:
    await asyncio.to_thread(sys.stdout.write, f"{string} ")
    return await asyncio.to_thread(sys.stdin.readline)


def random_subset(s, criterion=lambda _: random.randrange(2)):
    return set(filter(criterion, s))


async def ping_random_projects(projects: list[Project]) -> None:
    async with httpx.AsyncClient() as client:
        while True:
            subset_size = random.randrange(
                len(projects) // 10, len(projects) // 3
            ) // len(projects)
            print(f"[.] Waking up {subset_size}% of the {len(projects)} projects")
            subset = random_subset(projects, lambda _: random.random() < subset_size)
            for proj in subset:
                r = await client.get(f"https://{proj.name}.unstable.shuttleapp.rs")
                if r.status_code >= 400:
                    print(f"[!] Error while pinging project {proj.name}")
                    print(await r.text())

            sleep_time = random.randrange(30, 60 * 3)
            print(f"[.] Ping will sleep for {sleep_time} seconds..")
            await asyncio.sleep(sleep_time)


async def run_stress_test(runner: Runner, concurrency: int):
    print("[+] Deleting all the projects:")
    async for project in run_with_concurrency(concurrency, runner.delete, gen_projects(20)):
        print(f" [-] {project.name}: {project.state}")
    await asyncio.sleep(2)

    long_running = gen_projects(10)
    all_projects = long_running.copy()
    periodic_task = asyncio.create_task(periodic_check(runner, all_projects))

    def start(idle_minutes: int):
        async def start(project):
            await runner.start(project, idle_minutes=idle_minutes)
            # await runner.deploy(project)
            return project

        return start

    print("[+] Starting all long running projects:")
    async for project in run_with_concurrency(
        concurrency,
        start(idle_minutes=0),
        long_running,
        concurrency_jitter(concurrency, 5),
    ):
        print(f" [-] {project.name}: {project.state}")

    cch_projects = gen_projects(10, 20)
    all_projects.extend(cch_projects)

    print("[+] Starting all CCH projects:")
    async for project in run_with_concurrency(
        concurrency,
        start(idle_minutes=5),
        cch_projects,
        concurrency_jitter(concurrency, 5),
    ):
        print(f" [-] {project.name}: {project.state}")

    await ainput("Press enter to continue and deploy the projects")

    asyncio.create_task(ping_random_projects(all_projects))

    print("[+] Deploying all long running projects:")
    async for project in run_with_concurrency(
        concurrency,
        runner.deploy,
        long_running,
        concurrency_jitter(concurrency, 5),
    ):
        print(f" [-] {project.name}: {project.state}")

    await ainput("Press enter to continue and delete the projects")

    print("[+] Deleting all the projects:")
    async for project in run_with_concurrency(concurrency, runner.delete, all_projects):
        print(f" [-] {project.name}: {project.state}")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shuttle-wd", required=True)
    parser.add_argument("--max-concurrency", default=10, type=int)
    parser.add_argument("--num-projects", default=10, type=int)
    args = parser.parse_args()

    runner = Runner(args.shuttle_wd)
    projects = gen_projects(args.num_projects)
    n = args.max_concurrency

    await run_stress_test(runner, n)

    # print("[+] Projects state:")
    # async for project in run_with_concurrency(n, runner.status, projects):
    #     print(f"  [-] {project.name}: {project.state}")
    # print()
    #
    # periodic_task = asyncio.create_task(periodic_check(runner, projects))
    #
    # print("[+] Starting all the project:")
    # async for project in run_with_concurrency(n, runner.start, projects):
    #     print(f"  [-] {project.name}: {project.state}")
    # print()
    #
    # await asyncio.sleep(30)
    #
    # print("[+] Deploying all the project:")
    # async for project in run_with_concurrency(n, runner.deploy, projects):
    #     print(f"  [-] {project.name}: {project.state}")
    # print()
    #
    # await asyncio.sleep(30)
    #
    # print("[+] Stop all the project:")
    # async for project in run_with_concurrency(n, runner.stop, projects):
    #     print(f"  [-] {project.name}: {project.state}")
    # print()
    #
    # await asyncio.sleep(30)
    #
    # print("[+] Deleting all the project:")
    # async for project in run_with_concurrency(n, runner.delete, projects):
    #     print(f"[-] {project.name}: {project.state}")
    #
    # periodic_task.cancel()
    # await periodic_task


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
