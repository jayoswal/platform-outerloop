"""Read values from Docker Compose's fully resolved configuration."""

import json
import subprocess
from pathlib import Path
from typing import cast

COMPOSE_DIR = Path(__file__).resolve().parents[1] / "compose"


def service_environment_value(service: str, name: str) -> str:
    completed = subprocess.run(
        ["docker", "compose", "config", "--format", "json"],
        cwd=COMPOSE_DIR,
        check=True,
        capture_output=True,
        text=True,
    )
    config = cast(dict[str, object], json.loads(completed.stdout))
    services = cast(dict[str, object], config["services"])
    service_config = cast(dict[str, object], services[service])
    environment = cast(dict[str, object], service_config["environment"])
    value = environment.get(name)
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"{service} environment variable {name} is not configured.")
    return value
