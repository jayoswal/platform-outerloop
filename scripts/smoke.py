"""Cross-service P2/P3/P4 health, authentication, submission, decision,
notification, and employee-event fan-out smoke test."""

import asyncio
import datetime as dt
import json
import subprocess
import time
import uuid

import httpx
from compose_config import COMPOSE_DIR, service_environment_value

HEALTH_PATHS = (
    "/api/v1/identity/healthz",
    "/api/v1/time/healthz",
    "/api/v1/expense/healthz",
    "/api/v1/workflow/healthz",
)



async def prepare_event_probe(client: httpx.AsyncClient, queue: str) -> None:
    for exchange in ("time.events", "expense.events", "workflow.events"):
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
        ("workflow.events", "timesheet.approved"),
        ("workflow.events", "timesheet.rejected"),
        ("workflow.events", "expense.approved"),
        ("workflow.events", "expense.rejected"),
    ):
        response = await client.post(
            f"/api/bindings/%2F/e/{exchange}/q/{queue}",
            json={"routing_key": routing_key, "arguments": {}},
        )
        response.raise_for_status()


async def received_event_types(
    client: httpx.AsyncClient, queue: str, expected: set[str]
) -> set[str]:
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
        if expected <= seen:
            return seen
        await asyncio.sleep(0.25)
    return seen


async def wait_for_mailhog_message(to: str, subject_fragment: str) -> None:
    async with httpx.AsyncClient(base_url="http://localhost:8025", timeout=10) as mail:
        for _ in range(20):
            response = await mail.get("/api/v2/messages", params={"limit": 50})
            response.raise_for_status()
            for item in response.json().get("items", []):
                headers = item.get("Content", {}).get("Headers", {})
                recipients = [addr.lower() for addr in headers.get("To", [])]
                subject = "".join(headers.get("Subject", []))
                if any(to.lower() in recipient for recipient in recipients) and (
                    subject_fragment in subject
                ):
                    return
            await asyncio.sleep(0.5)
    raise RuntimeError(f"No MailHog message to {to} with subject containing {subject_fragment!r}.")


async def find_pending_approval(
    client: httpx.AsyncClient, headers: dict[str, str], subject_id: str
) -> dict[str, object]:
    for _ in range(20):
        response = await client.get(
            "/api/v1/approvals", headers=headers, params={"status": "PENDING"}
        )
        response.raise_for_status()
        for item in response.json()["items"]:
            if item["subject_id"] == subject_id:
                return item
        await asyncio.sleep(0.25)
    raise RuntimeError(f"No pending approval appeared for subject {subject_id}.")


async def wait_for_time_profile(
    client: httpx.AsyncClient, headers: dict[str, str], employee_id: str
) -> dict[str, object]:
    for _ in range(40):
        response = await client.get(
            f"/api/v1/time/profiles/{employee_id}", headers=headers
        )
        if response.status_code == 200:
            return response.json()
        if response.status_code != 404:
            response.raise_for_status()
        await asyncio.sleep(0.25)
    raise RuntimeError(f"Time profile for {employee_id} never provisioned.")


async def wait_for_expense_profile(
    client: httpx.AsyncClient, headers: dict[str, str], employee_id: str
) -> dict[str, object]:
    for _ in range(40):
        response = await client.get(
            f"/api/v1/expense/profiles/{employee_id}", headers=headers
        )
        if response.status_code == 200:
            return response.json()
        if response.status_code != 404:
            response.raise_for_status()
        await asyncio.sleep(0.25)
    raise RuntimeError(f"Expense profile for {employee_id} never provisioned.")


def wait_for_workflow_employee_read(employee_id: str, expected_status: str) -> dict[str, str]:
    postgres_user = service_environment_value("postgres", "POSTGRES_USER")
    for _ in range(40):
        completed = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                postgres_user,
                "-d",
                "workflow_db",
                "-t",
                "-A",
                "-F",
                ",",
                "-c",
                (
                    "SELECT email, full_name, status FROM employee_read "
                    f"WHERE id = '{employee_id}'"
                ),
            ],
            cwd=COMPOSE_DIR,
            check=True,
            capture_output=True,
            text=True,
        )
        row = completed.stdout.strip()
        if row:
            email, full_name, status = row.split(",")
            if status == expected_status:
                return {"email": email, "full_name": full_name, "status": status}
        time.sleep(0.25)
    raise RuntimeError(f"Workflow employee_read row for {employee_id} never synced.")


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

                submitted_events = await received_event_types(
                    rabbit, queue, {"timesheet.submitted", "expense.submitted"}
                )
                if not {"timesheet.submitted", "expense.submitted"} <= submitted_events:
                    missing = {"timesheet.submitted", "expense.submitted"} - submitted_events
                    raise RuntimeError(f"Submitted events missing from RabbitMQ: {sorted(missing)}")
                print("PASS RabbitMQ submitted events")

                grace_login = await client.post(
                    "/api/v1/auth/login",
                    json={"email": "grace@atlas.dev", "password": password},
                )
                grace_login.raise_for_status()
                grace_headers = {
                    "Authorization": "Bearer " + grace_login.json()["token"]
                }
                print("PASS grace login (manager)")

                timesheet_approval = await find_pending_approval(
                    client, grace_headers, timesheet_id
                )
                if timesheet_approval["policy_flags"][0]["type"] != "OVERTIME_THRESHOLD":
                    raise RuntimeError("Timesheet approval was not flagged for overtime.")
                decided_timesheet = await client.post(
                    f"/api/v1/approvals/{timesheet_approval['id']}/decision",
                    headers=grace_headers,
                    json={"decision": "APPROVE"},
                )
                decided_timesheet.raise_for_status()
                if decided_timesheet.json()["status"] != "APPROVED":
                    raise RuntimeError("Timesheet approval decision did not persist.")
                print("PASS timesheet approval decision (approve)")

                report_approval = await find_pending_approval(client, grace_headers, report_id)
                if report_approval["policy_flags"][0]["type"] != "PER_RECEIPT_CAP":
                    raise RuntimeError("Expense approval was not flagged for the receipt cap.")
                decided_report = await client.post(
                    f"/api/v1/approvals/{report_approval['id']}/decision",
                    headers=grace_headers,
                    json={"decision": "REJECT", "comment": "Smoke test rejection"},
                )
                decided_report.raise_for_status()
                if decided_report.json()["status"] != "REJECTED":
                    raise RuntimeError("Expense approval decision did not persist.")
                print("PASS expense approval decision (reject)")

                decision_events = await received_event_types(
                    rabbit, queue, {"timesheet.approved", "expense.rejected"}
                )
                if not {"timesheet.approved", "expense.rejected"} <= decision_events:
                    missing = {"timesheet.approved", "expense.rejected"} - decision_events
                    raise RuntimeError(f"Decision events missing from RabbitMQ: {sorted(missing)}")
                print("PASS RabbitMQ decision events")

                for _ in range(20):
                    finalized_timesheet = await client.get(
                        f"/api/v1/time/timesheets/{timesheet_id}", headers=headers
                    )
                    finalized_timesheet.raise_for_status()
                    finalized_report = await client.get(
                        f"/api/v1/expense/reports/{report_id}", headers=headers
                    )
                    finalized_report.raise_for_status()
                    if (
                        finalized_timesheet.json()["status"] == "APPROVED"
                        and finalized_report.json()["status"] == "REJECTED"
                    ):
                        break
                    await asyncio.sleep(0.25)
                else:
                    raise RuntimeError(
                        "Timesheet/expense did not finalize after the approval decision."
                    )
                print("PASS timesheet/expense finalized from decision events")

                admin_login = await client.post(
                    "/api/v1/auth/login",
                    json={"email": "admin@atlas.dev", "password": password},
                )
                admin_login.raise_for_status()
                admin_headers = {
                    "Authorization": "Bearer " + admin_login.json()["token"]
                }
                print("PASS admin login (HR_ADMIN)")

                new_hire_email = f"smoke-{run_id}@atlas.dev"
                created_employee = await client.post(
                    "/api/v1/identity/employees",
                    headers=admin_headers,
                    json={
                        "email": new_hire_email,
                        "full_name": "Smoke Testerson",
                        "grade": "IC1",
                        "cost_center": "CC-100",
                        "manager_id": "10000000-0000-4000-8000-000000000001",
                        "home_currency": "USD",
                        "roles": ["EMPLOYEE"],
                    },
                )
                created_employee.raise_for_status()
                new_employee = created_employee.json()
                new_employee_id = new_employee["id"]
                if new_employee["provisioning"] != "PROVISIONING":
                    raise RuntimeError("New employee response missing provisioning marker.")
                print("PASS POST /api/v1/identity/employees")

                time_profile = await wait_for_time_profile(
                    client, admin_headers, new_employee_id
                )
                if time_profile["employee_id"] != new_employee_id:
                    raise RuntimeError("Time profile id did not match the new employee.")
                print("PASS employee.created fan-out to svc-time profile")

                expense_profile = await wait_for_expense_profile(
                    client, admin_headers, new_employee_id
                )
                if expense_profile["employee_id"] != new_employee_id:
                    raise RuntimeError("Expense profile id did not match the new employee.")
                print("PASS employee.created fan-out to svc-expense profile")

                workflow_row = wait_for_workflow_employee_read(new_employee_id, "ACTIVE")
                if workflow_row["email"] != new_hire_email:
                    raise RuntimeError("Workflow employee_read email did not match.")
                print("PASS employee.created fan-out to svc-workflow employee_read")

                # Consumers upsert by primary key, so re-reading the same
                # already-provisioned profile must return an unchanged row
                # (the same guarantee that makes redelivery of employee.created
                # a safe no-op).
                replayed_time_profile = await wait_for_time_profile(
                    client, admin_headers, new_employee_id
                )
                if replayed_time_profile != time_profile:
                    raise RuntimeError(
                        "Re-reading the time profile after redelivery-safe consumers "
                        "changed unexpectedly."
                    )
                print("PASS employee-event consumers remain idempotent on re-read")

                deactivated = await client.post(
                    f"/api/v1/identity/employees/{new_employee_id}/deactivate",
                    headers=admin_headers,
                )
                deactivated.raise_for_status()
                if deactivated.json()["status"] != "INACTIVE":
                    raise RuntimeError("Deactivate response did not report INACTIVE.")
                deactivated_row = wait_for_workflow_employee_read(new_employee_id, "INACTIVE")
                if deactivated_row["status"] != "INACTIVE":
                    raise RuntimeError("Workflow employee_read status was not synced to INACTIVE.")
                print("PASS employee.deactivated fan-out to svc-workflow employee_read")

            await wait_for_mailhog_message("grace@atlas.dev", "approval requested")
            print("PASS MailHog approval-request email")
        finally:
            response = await rabbit.delete(f"/api/queues/%2F/{queue}")
            if response.status_code not in (204, 404):
                response.raise_for_status()


if __name__ == "__main__":
    asyncio.run(main())
