"""Seed entrypoint; domain seed calls are added as P1-P4 services become available."""

import asyncio

import httpx


async def main() -> None:
    async with httpx.AsyncClient(base_url="http://localhost:8080", timeout=10) as client:
        response = await client.get("/api/v1/identity/healthz")
        response.raise_for_status()
    print("Atlas services reachable; domain seed data is introduced in P1.")


if __name__ == "__main__":
    asyncio.run(main())

