"""Synchronize install_requires in setup.py from Pipfile.lock without hard pins (except allowlist)."""

from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PIPFILE = ROOT / "Pipfile"
LOCKFILE = ROOT / "Pipfile.lock"
SETUP = ROOT / "setup.py"
PINNED_PACKAGES: set[str] = {"daplug-core"}


def load_top_level_packages(pipfile_path: Path) -> list[str]:
    packages: list[str] = []
    in_packages = False
    for raw_line in pipfile_path.read_text().splitlines():
        line = raw_line.strip()
        if line.startswith("[packages]"):
            in_packages = True
            continue
        if in_packages and line.startswith("["):
            break
        if not in_packages or not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        name, _ = line.split("=", 1)
        packages.append(name.strip().strip('"').strip("'"))
    return packages


def next_major(version: str) -> str | None:
    parts = version.split(".")
    try:
        major = int(parts[0])
    except (ValueError, TypeError):
        return None
    return str(major + 1)


def build_requirement(name: str, entry: dict) -> str:
    version_spec: str = entry.get("version", "")
    markers: str | None = entry.get("markers")
    clean_version = version_spec.lstrip("=")

    if name in PINNED_PACKAGES:
        requirement = f"{name}{version_spec}"
    else:
        upper = next_major(clean_version)
        requirement = f"{name}>={clean_version}"
        if upper:
            requirement = f"{requirement},<{upper}"

    if markers:
        requirement = f"{requirement}; {markers}"
    return requirement


def render_install_requires(requirements: list[str]) -> str:
    lines = ["    install_requires=["]
    for req in requirements:
        lines.append(f'        "{req}",')
    lines.append("    ],")
    return "\n".join(lines)


def update_setup_install_requires(requirements: list[str]) -> None:
    setup_text = SETUP.read_text()
    block = render_install_requires(requirements)
    pattern = re.compile(r"(?ms)^    install_requires=\[\n.*?\n    \],")
    new_text, count = pattern.subn(block, setup_text)
    if count == 0:
        raise RuntimeError("install_requires block not found in setup.py")
    SETUP.write_text(new_text)


def main() -> None:
    packages = load_top_level_packages(PIPFILE)
    lock = json.loads(LOCKFILE.read_text())
    default = lock.get("default", {})

    requirements: list[str] = []
    for name in packages:
        entry = default.get(name)
        if not entry:
            raise RuntimeError(f"Package {name!r} not found in Pipfile.lock")
        requirements.append(build_requirement(name, entry))

    update_setup_install_requires(requirements)
    print("Updated setup.py install_requires:")
    for req in requirements:
        print(f" - {req}")


if __name__ == "__main__":
    main()
