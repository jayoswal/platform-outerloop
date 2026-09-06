"""Cross-service P1 health and authenticated-session smoke test."""

import asyncio
import uuid

import httpx
from compose_config import service_environment_value

HEALTH_PATHS = (
    "/api/v1/identity/healthz",
    "/api/v1/time/healthz",
    "/api/v1/expense/healthz",
    "/api/v1/workflow/healthz",
)


async def main() -> None:
    correlation_id = str(uuid.uuid4())
    password = service_environment_value("svc-identity", "DEMO_PASSWORD")
    async with httpx.AsyncClient(base_url="http://localhost:8080", timeout=10) as client:
        for path in HEALTH_PATHS:
            response = await client.get(path)
            response.raise_for_status()
            print(f"PASS {path}")

        login = await client.post(
            "/api/v1/auth/login",
            headers={"X-Correlation-Id": correlation_id},
            json={"email": "ada@atlas.dev", "password": password},
        )
        login.raise_for_status()
        if login.headers.get("X-Correlation-Id") != correlation_id:
            raise RuntimeError("Identity service did not echo the correlation ID.")
        token = login.json()["token"]
        print("PASS /api/v1/auth/login")

        current_user = await client.get(
            "/api/v1/identity/me",
            headers={"Authorization": f"Bearer {token}"},
        )
        current_user.raise_for_status()
        employee = current_user.json()
        if employee["email"] != "ada@atlas.dev" or employee["roles"] != ["EMPLOYEE"]:
            raise RuntimeError("Authenticated employee response did not match seed data.")
        print("PASS /api/v1/identity/me")


if __name__ == "__main__":
    asyncio.run(main())
