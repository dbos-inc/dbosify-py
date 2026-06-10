#!/usr/bin/env python3
"""
Release automation script for temporal-dbos.

This script automates the release process:
1. Validates the repository state (clean, on main, up-to-date)
2. Creates a git tag with the version number
3. Creates a release branch (release/v{version})
4. Pushes both to origin

Usage:
    python make_release.py                    # Auto-generate next version
    python make_release.py --version 0.2.0   # Specify version explicitly

After running this script, trigger the "Publish to PyPI" GitHub Action
from the release branch to publish the package.
"""

import os
import re
import sys

from git import Repo


def make_release(version_number: str | None = None) -> None:
    """Create and push a new release."""
    repo = Repo(os.getcwd())

    # Validate repository state
    if repo.is_dirty():
        raise Exception(
            "Local git repository is not clean. Commit or stash changes first."
        )

    if repo.active_branch.name != "main":
        raise Exception(
            f"Can only make a release from main (currently on {repo.active_branch.name})"
        )

    # Check if local is up-to-date with remote
    repo.remotes.origin.fetch()
    remote_branch = repo.references[f"origin/{repo.active_branch.name}"]
    local_commit = repo.active_branch.commit
    remote_commit = remote_branch.commit

    if local_commit != remote_commit:
        raise Exception(
            f"Your local branch {repo.active_branch.name} is not up to date with origin. "
            "Please pull or push as needed."
        )

    # Determine version number
    if version_number is None:
        version_number = guess_next_version(repo)
        print(f"Auto-generated version: {version_number}")

    # Validate version format
    version_pattern = r"^\d+\.\d+\.\d+$"
    if not re.match(version_pattern, version_number):
        raise Exception(
            f"Invalid version number: {version_number}. "
            "Must be in format X.Y.Z (e.g., 0.2.0)"
        )

    # Check if tag already exists
    if version_number in [tag.name for tag in repo.tags]:
        raise Exception(f"Tag {version_number} already exists")

    print(f"\nCreating release {version_number}...")
    print("-" * 40)

    create_and_push_release_tag(repo=repo, version_number=version_number)
    create_and_push_release_branch(repo=repo, version_number=version_number)

    print("-" * 40)
    print(f"\nRelease {version_number} created successfully!")
    print("\nNext steps:")
    print(f"  1. Go to GitHub Actions")
    print(
        f"  2. Run 'Publish to PyPI' workflow from the release/v{version_number} branch"
    )


def guess_next_version(repo: Repo) -> str:
    """Guess the next version by finding the latest tag and incrementing minor version."""
    tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime, reverse=True)

    last_tag = None
    for tag in tags:
        # Only consider tags that are ancestors of main
        try:
            if repo.is_ancestor(tag.commit, repo.heads.main.commit):
                # Only consider semantic version tags
                if re.match(r"^\d+\.\d+\.\d+$", tag.name):
                    last_tag = tag.name
                    break
        except Exception:
            continue

    if last_tag is None:
        # No previous tags, start at 0.1.0
        return "0.1.0"

    major, minor, patch = map(int, last_tag.split("."))
    minor += 1
    patch = 0  # Reset patch when incrementing minor
    return f"{major}.{minor}.{patch}"


def create_and_push_release_tag(repo: Repo, version_number: str) -> None:
    """Create a git tag and push it to origin."""
    release_tag = repo.create_tag(version_number)
    print(f"Created tag: {version_number}")

    push_info = repo.remote("origin").push(release_tag)
    if push_info[0].flags & push_info[0].ERROR:
        raise Exception(f"Failed to push tag: {push_info[0].summary}")
    print(f"Pushed tag: {version_number}")


def create_and_push_release_branch(repo: Repo, version_number: str) -> None:
    """Create a release branch and push it to origin."""
    branch_name = f"release/v{version_number}"

    # Create branch from main
    new_branch = repo.create_head(branch_name, repo.heads["main"])
    print(f"Created branch: {branch_name}")

    # Checkout the new branch
    new_branch.checkout()

    # Push to origin
    push_info = repo.remote("origin").push(new_branch)
    if push_info[0].flags & push_info[0].ERROR:
        raise Exception(f"Failed to push branch: {push_info[0].summary}")
    print(f"Pushed branch: {branch_name}")


def main() -> None:
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Create a new release for temporal-dbos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--version",
        "-v",
        type=str,
        default=None,
        help="Version number (e.g., 0.2.0). If not provided, auto-generates next version.",
    )

    args = parser.parse_args()

    try:
        make_release(version_number=args.version)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
