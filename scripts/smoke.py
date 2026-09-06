"""Cross-service P2 health, authentication, submission, and event smoke test."""

import asyncio
import datetime as dt
import json
import uuid

import httpx
from compose_config import service_environment_value

HEALTH_PATHS = (
    "/api/v1/identity/healthz",
    "/api/v1/time/healthz",
    "/api/v1/expense/healthz",
    "/api/v1/workflow/healthz",
)


async def prepare_event_probe(client: httpx.AsyncClient, queue: str) -> None:
    for exchange in ("time.events", "expense.events"):
        response = await client.put(
            f"/api/exchanges/%2F/{exchange}",
            json={"type": "topic", "durable": True, "auto_delete": False, "arguments": {}},
        )
        response.raise_for_status()
    response = await client.put(
        f"/api/queues/%2F/{queue}",
        json={"durable": False, "auto_delete": False, "arguments": {}},
    )
    response.raise_for_status()
    for exchange, routing_key in (
        ("time.events", "timesheet.submitted"),
        ("expense.events", "expense.submitted"),
    ):
        response = await client.post(
            f"/api/bindings/%2F/e/{exchange}/q/{queue}",
            json={"routing_key": routing_key, "arguments": {}},
        )
        response.raise_for_status()


async def received_event_types(client: httpx.AsyncClient, queue: str) -> set[str]:
    seen: set[str] = set()
    for _ in range(20):
        response = await client.post(
            f"/api/queues/%2F/{queue}/get",
            json={
                "count": 10,
                "ackmode": "ack_requeue_false",
                "encoding": "auto",
                "truncate": 50000,
            },
        )
        response.raise_for_status()
        messages = response.json()
        seen.update(
            json.loads(message["payload"])["type"]
            for message in messages
            if isinstance(message.get("payload"), str)
        )
        if {"timesheet.submitted", "expense.submitted"} <= seen:
            return seen
        await asyncio.sleep(0.25)
    return seen


async def main() -> None:
    correlation_id = str(uuid.uuid4())
    run_id = uuid.uuid4()
    queue = f"atlas-smoke-{run_id}"
    rabbit_user = service_environment_value("rabbitmq", "RABBITMQ_DEFAULT_USER")
    rabbit_password = service_environment_value("rabbitmq", "RABBITMQ_DEFAULT_PASS")

    async with httpx.AsyncClient(
        base_url="http://localhost:15672",
        auth=(rabbit_user, rabbit_password),
        timeout=10,
    ) as rabbit:
        await prepare_event_probe(rabbit, queue)
        try:
            async with httpx.AsyncClient(
                base_url="http://localhost:8080", timeout=10
            ) as client:
                for path in HEALTH_PATHS:
                    response = await client.get(path)
                    response.raise_for_status()
                    print(f"PASS {path}")

                password = service_environment_value(
                    "svc-identity", "DEMO_PASSWORD"
                )
                login = await client.post(
                    "/api/v1/auth/login",
                    headers={"X-Correlation-Id": correlation_id},
                    json={"email": "ada@atlas.dev", "password": password},
                )
                login.raise_for_status()
                if login.headers.get("X-Correlation-Id") != correlation_id:
                    raise RuntimeError("Identity service did not echo the correlation ID.")
                token = login.json()["token"]
                headers = {"Authorization": "Bearer " + token}
                print("PASS /api/v1/auth/login")

                current_user = await client.get(
                    "/api/v1/identity/me", headers=headers
                )
                current_user.raise_for_status()
                employee = current_user.json()
                if (
                    employee["email"] != "ada@atlas.dev"
                    or employee["roles"] != ["EMPLOYEE"]
                ):
                    raise RuntimeError(
                        "Authenticated employee response did not match seed data."
                    )
                print("PASS /api/v1/identity/me")

                for attempt in range(10000):
                    start = dt.date(2030, 1, 7) + dt.timedelta(
                        weeks=(run_id.int + attempt) % 10000
                    )
                    end = start + dt.timedelta(days=6)
                    entries = [
                        {
                            "work_date": (
                                start + dt.timedelta(days=day)
                            ).isoformat(),
                            "hours": 10,
                            "project_code": "ATLAS",
                            "note": "P2 integration smoke",
                        }
                        for day in range(5)
                    ]
                    created_timesheet = await client.post(
                        "/api/v1/time/timesheets",
                        headers=headers,
                        json={
                            "period_start": start.isoformat(),
                            "period_end": end.isoformat(),
                            "entries": entries,
                        },
                    )
                    if created_timesheet.status_code != 409:
                        created_timesheet.raise_for_status()
                        break
                else:
                    raise RuntimeError("No unused smoke-test timesheet period remains.")
                timesheet_id = created_timesheet.json()["id"]
                submitted_timesheet = await client.post(
                    f"/api/v1/time/timesheets/{timesheet_id}/submit",
                    headers=headers,
                )
                submitted_timesheet.raise_for_status()
                timesheet = submitted_timesheet.json()
                if (
                    timesheet["status"] != "PENDING_APPROVAL"
                    or timesheet["total_hours"] != 50
                    or timesheet["overtime_hours"] != 10
                ):
                    raise RuntimeError("Timesheet totals or status are incorrect.")
                print("PASS timesheet create/submit")

                categories_response = await client.get(
                    "/api/v1/expense/categories", headers=headers
                )
                categories_response.raise_for_status()
                categories = categories_response.json()
                if not categories:
                    raise RuntimeError("Expense seed did not create categories.")
                created_report = await client.post(
                    "/api/v1/expense/reports",
                    headers=headers,
                    json={"title": f"P2 smoke {run_id}"},
                )
                created_report.raise_for_status()
                report_id = created_report.json()["id"]
                added_line = await client.post(
                    f"/api/v1/expense/reports/{report_id}/lines",
                    headers=headers,
                    json={
                        "category_id": categories[0]["id"],
                        "amount_minor": 12345,
                        "currency": "GBP",
                        "receipt_url": "https://example.test/receipt",
                        "spent_on": start.isoformat(),
                    },
                )
                added_line.raise_for_status()
                submitted_report = await client.post(
                    f"/api/v1/expense/reports/{report_id}/submit",
                    headers=headers,
                )
                submitted_report.raise_for_status()
                report = submitted_report.json()
                if (
                    report["status"] != "PENDING_APPROVAL"
                    or report["total_home_minor"] != 12345
                ):
                    raise RuntimeError("Expense total or status is incorrect.")
                print("PASS expense create/line/submit")

            event_types = await received_event_types(rabbit, queue)
            expected = {"timesheet.submitted", "expense.submitted"}
            if not expected <= event_types:
                raise RuntimeError(
                    f"Submitted events missing from RabbitMQ: {sorted(expected - event_types)}"
                )
            print("PASS RabbitMQ submitted events")
        finally:
            response = await rabbit.delete(f"/api/queues/%2F/{queue}")
            if response.status_code not in (204, 404):
                response.raise_for_status()


if __name__ == "__main__":
    asyncio.run(main())
