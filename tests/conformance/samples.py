"""Management of the temporalio/samples-python conformance corpus.

The repo is cloned at a pinned commit into a cache under the system temp
directory (kept out of the repo tree). Samples are prepared for execution by
the mechanical migration step: rewrite the import root ``temporalio`` ->
``temporal_dbos``. Connection setup (the documented remaining migration
delta) is adapted at runtime by runner.py.
"""

import re
import subprocess
import tempfile
from pathlib import Path

SAMPLES_REPO_URL = "https://github.com/temporalio/samples-python.git"
PINNED_COMMIT = "5ceb3c80daf8d044be0e5693d6f07b1b0c8465e5"
CACHE_DIR = Path(tempfile.gettempdir()) / "temporal-dbos-conformance" / "samples-python"

_IMPORT_ROOT_RE = re.compile(r"\btemporalio\b")


def ensure_samples() -> Path:
    """Clone (or update) the pinned samples-python checkout; returns its
    root. Needs network on first use; cached afterwards.
    """
    if not CACHE_DIR.exists():
        CACHE_DIR.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["git", "clone", "--depth", "1", SAMPLES_REPO_URL, str(CACHE_DIR)],
            check=True,
            capture_output=True,
        )
    head = subprocess.run(
        ["git", "-C", str(CACHE_DIR), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if head != PINNED_COMMIT:
        subprocess.run(
            [
                "git",
                "-C",
                str(CACHE_DIR),
                "fetch",
                "--depth",
                "1",
                "origin",
                PINNED_COMMIT,
            ],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "-C", str(CACHE_DIR), "checkout", PINNED_COMMIT],
            check=True,
            capture_output=True,
        )
    return CACHE_DIR


def rewrite_sample(source: Path, dest_dir: Path) -> Path:
    """The mechanical migration: swap the import root. Returns the rewritten
    file's path.
    """
    rewritten = _IMPORT_ROOT_RE.sub("temporal_dbos", source.read_text())
    dest = dest_dir / source.name
    dest.write_text(rewritten)
    return dest
