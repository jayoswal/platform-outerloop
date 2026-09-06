"""Validate Atlas runtime, dependency, image, and GitHub Action pins."""

from __future__ import annotations

import json
import re
import sys
import tomllib
from pathlib import Path

SERVICE_REPOS = ("svc-identity", "svc-time", "svc-expense", "svc-workflow")
CODE_REPOS = (*SERVICE_REPOS, "hrms-web", "platform-outerloop")
EXACT_VERSION = re.compile(r"^\d+(?:\.\d+)+(?:[A-Za-z0-9.+-]*)?$")
IMAGE_REF = re.compile(r"^[^@\s]+:[^@\s]+@sha256:[0-9a-f]{64}$")
ACTION_REF = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")


def normalized_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def requirement_parts(requirement: str) -> tuple[str, str] | None:
    match = re.fullmatch(
        r"([A-Za-z0-9_.-]+)(?:\[[A-Za-z0-9_,.-]+\])?==(.+)", requirement
    )
    if match is None:
        return None
    return normalized_name(match.group(1)), match.group(2)


def check_python_project(project: Path, *, require_lock: bool = True) -> list[str]:
    errors: list[str] = []
    version_file = project / ".python-version"
    if not version_file.is_file() or not EXACT_VERSION.fullmatch(version_file.read_text().strip()):
        errors.append(f"{project}: missing or non-exact .python-version")

    pyproject = project / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text())
    requirements = list(data["project"].get("dependencies", []))
    for group in data.get("dependency-groups", {}).values():
        requirements.extend(group)

    parsed: list[tuple[str, str]] = []
    for requirement in requirements:
        parts = requirement_parts(requirement)
        if parts is None or not EXACT_VERSION.fullmatch(parts[1]):
            errors.append(f"{pyproject}: non-exact requirement {requirement!r}")
        else:
            parsed.append(parts)

    lock_path = project / "uv.lock"
    if require_lock:
        if not lock_path.is_file():
            errors.append(f"{project}: missing uv.lock")
        else:
            lock = tomllib.loads(lock_path.read_text())
            locked = {
                normalized_name(package["name"]): package["version"]
                for package in lock["package"]
                if "version" in package
            }
            for name, version in parsed:
                if locked.get(name) != version:
                    errors.append(
                        f"{pyproject}: {name} pins {version}, lock has {locked.get(name)!r}"
                    )
    return errors


def check_frontend(project: Path) -> list[str]:
    errors: list[str] = []
    nvmrc = project / ".nvmrc"
    if not nvmrc.is_file() or not EXACT_VERSION.fullmatch(nvmrc.read_text().strip()):
        errors.append(f"{project}: missing or non-exact .nvmrc")

    manifest_path = project / "package.json"
    lock_path = project / "package-lock.json"
    if not lock_path.is_file():
        return [*errors, f"{project}: missing package-lock.json"]

    manifest = json.loads(manifest_path.read_text())
    lock = json.loads(lock_path.read_text())
    for group in ("dependencies", "devDependencies"):
        for name, version in manifest.get(group, {}).items():
            if not EXACT_VERSION.fullmatch(version):
                errors.append(f"{manifest_path}: non-exact requirement {name}={version}")
                continue
            locked = lock["packages"].get(f"node_modules/{name}", {}).get("version")
            if locked != version:
                errors.append(f"{manifest_path}: {name} pins {version}, lock has {locked!r}")
    return errors


def check_image_refs(workspace: Path) -> list[str]:
    errors: list[str] = []
    python_files = [workspace / repo / "Dockerfile" for repo in SERVICE_REPOS]
    python_files.append(
        workspace
        / "platform-outerloop/templates/service/{{cookiecutter.service_slug}}/Dockerfile"
    )
    files = [*python_files, workspace / "hrms-web/Dockerfile"]
    compose = workspace / "platform-outerloop/compose/docker-compose.yml"

    refs: list[tuple[Path, str]] = []
    for path in files:
        content = path.read_text()
        for line in content.splitlines():
            if line.startswith("FROM "):
                refs.append((path, line.split()[1]))
        if path in python_files and "pip install --no-cache-dir uv==0.12.10" not in content:
            errors.append(f"{path}: container uv installer is not pinned to 0.12.10")
    for line in compose.read_text().splitlines():
        match = re.match(r"\s*image:\s*(\S+)", line)
        if match:
            refs.append((compose, match.group(1)))

    for path, ref in refs:
        if not IMAGE_REF.fullmatch(ref):
            errors.append(f"{path}: image is not exact tag plus digest: {ref}")
    return errors


def check_actions(workspace: Path) -> list[str]:
    errors: list[str] = []
    for repo in CODE_REPOS:
        workflows = workspace / repo / ".github/workflows"
        if not workflows.is_dir():
            continue
        for path in workflows.glob("*.y*ml"):
            for line in path.read_text().splitlines():
                match = re.match(r"\s*-\s+uses:\s*(\S+)", line)
                if match and not match.group(1).startswith("./"):
                    ref = match.group(1)
                    if not ACTION_REF.fullmatch(ref):
                        errors.append(f"{path}: action is not SHA-pinned: {ref}")
    return errors


def main() -> int:
    workspace = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    errors: list[str] = []
    for repo in SERVICE_REPOS:
        errors.extend(check_python_project(workspace / repo))
    errors.extend(check_python_project(workspace / "platform-outerloop"))
    errors.extend(
        check_python_project(
            workspace
            / "platform-outerloop/templates/service/{{cookiecutter.service_slug}}",
            require_lock=False,
        )
    )
    errors.extend(check_frontend(workspace / "hrms-web"))
    errors.extend(check_image_refs(workspace))
    errors.extend(check_actions(workspace))

    if errors:
        print("\n".join(f"ERROR: {error}" for error in errors), file=sys.stderr)
        return 1
    print("All Atlas runtime, dependency, image, and Action pins are exact.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
