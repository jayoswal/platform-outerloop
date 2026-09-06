"""Idempotently seed Atlas demo data through service-owned seed commands."""

import asyncio
import subprocess
import time

import httpx
from compose_config import COMPOSE_DIR, service_environment_value


def wait_for_identity() -> None:
    for _ in range(60):
        try:
            response = httpx.get(
                "http://localhost:8080/api/v1/identity/healthz",
                timeout=2,
            )
            if response.status_code == 200 and response.json() == {
                "status": "ok",
                "service": "svc-identity",
            }:
                return
        except (httpx.HTTPError, ValueError):
            pass
        time.sleep(1)
    raise RuntimeError("Identity service did not become ready within 60 seconds.")


def seed_identity() -> None:
    wait_for_identity()
    completed = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "svc-identity",
            "uv",
            "run",
            "--no-sync",
            "python",
            "-m",
            "app.seed",
        ],
        cwd=COMPOSE_DIR,
        check=True,
        capture_output=True,
        text=True,
    )
    print(completed.stdout.strip())


async def verify_login() -> None:
    password = service_environment_value("svc-identity", "DEMO_PASSWORD")
    async with httpx.AsyncClient(base_url="http://localhost:8080", timeout=10) as client:
        response = await client.post(
            "/api/v1/auth/login",
            json={"email": "ada@atlas.dev", "password": password},
        )
        response.raise_for_status()
        if not response.json().get("token"):
            raise RuntimeError("Identity seed verification did not return a token.")
    print("Verified demo login for ada@atlas.dev.")


async def main() -> None:
    seed_identity()
    await verify_login()


if __name__ == "__main__":
    asyncio.run(main())
