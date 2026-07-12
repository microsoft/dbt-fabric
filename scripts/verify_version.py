#!/usr/bin/env python
"""Verify that a git tag (e.g. from GITHUB_REF_NAME) matches the package version.

This replaces the old setup.py "verify" command that used to run as a
setuptools install hook. It's now a standalone script invoked directly
from the release workflow, per the recommendation in
https://github.com/microsoft/dbt-fabric/issues/371.

Usage: python scripts/verify_version.py <tag>
       (e.g. python scripts/verify_version.py "$GITHUB_REF_NAME")
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
VERSION_PATH = THIS_DIR.parent / "dbt" / "adapters" / "fabric" / "__version__.py"
VERSION_PATTERN = re.compile(r"""version\s*=\s*["'](.+)["']""")


def get_package_version() -> str:
    contents = VERSION_PATH.read_text()
    match = VERSION_PATTERN.search(contents)
    if match is None:
        raise ValueError(f"invalid version at {VERSION_PATH}")
    return match.group(1)


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python scripts/verify_version.py <tag>", file=sys.stderr)
        return 2

    tag = sys.argv[1]
    tag_without_prefix = tag[1:] if tag.startswith("v") else tag
    package_version = get_package_version()

    if tag_without_prefix != package_version:
        print(
            f"Git tag: {tag_without_prefix} does not match the version of this app: "
            f"{package_version}",
            file=sys.stderr,
        )
        return 1

    print(f"Version {package_version} matches tag {tag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
