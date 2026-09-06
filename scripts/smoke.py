"""Cross-service health smoke test."""

import asyncio

import httpx

PATHS = (
    "/api/v1/identity/healthz",
    "/api/v1/time/healthz",
    "/api/v1/expense/healthz",
    "/api/v1/workflow/healthz",
)


async def main() -> None:
    async with httpx.AsyncClient(base_url="http://localhost:8080", timeout=10) as client:
        for path in PATHS:
            response = await client.get(path)
            response.raise_for_status()
            print(f"PASS {path}")


if __name__ == "__main__":
    asyncio.run(main())

