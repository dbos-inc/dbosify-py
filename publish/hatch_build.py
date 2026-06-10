"""
Custom Hatch build hook for branch-aware versioning.

1. Release versions: From release/* branches, version is the git tag (e.g., 0.2.0)
2. Preview versions: From main, PEP440 alpha releases (e.g., 0.3.0a6)
3. Test versions: From feature branches, local versions with git hash (e.g., 0.3.0a6+g1a2b3c4)
"""

import os
import re
from typing import Any

from git import Repo
from hatchling.version.source.plugin.interface import VersionSourceInterface


class DBOSVersionSource(VersionSourceInterface):
    PLUGIN_NAME = "dbos-vcs"

    def get_version_data(self) -> dict[str, Any]:
        """Get version data from git."""
        version = self._compute_version()
        return {"version": version}

    def _compute_version(self) -> str:
        """Compute version based on git state and branch."""
        repo = Repo(self.root)

        branch = self._get_branch(repo)
        tag = self._get_latest_tag(repo)
        distance = self._get_distance_from_tag(repo, tag) if tag else None
        node = f"g{repo.head.commit.hexsha[:7]}"

        is_release = "release" in branch if branch else False
        is_preview = branch == "main"

        next_version = self._guess_next_version(tag) if tag else "0.1.0"

        if distance is None or distance == 0:
            # On a tag or no commits since tag
            if is_release and tag:
                return tag
            elif is_preview:
                return f"{next_version}a0"
            else:
                return f"{next_version}a0+{node}"
        else:
            # Commits since last tag
            if is_release:
                raise ValueError(
                    f"Release branches may only publish tagged releases. "
                    f"Current distance from tag: {distance} commits. "
                    f"Please create a new tag for this release."
                )
            elif is_preview:
                return f"{next_version}a{distance}"
            else:
                # Feature branch
                return f"{next_version}a{distance}+{node}"

    def _get_branch(self, repo: Repo) -> str | None:
        """Get the current branch name."""
        # Try getting branch from git
        if not repo.head.is_detached:
            return repo.active_branch.name

        # In detached HEAD state (common in CI), get branch from environment
        # GitHub Actions
        if ref := os.environ.get("GITHUB_REF"):
            if ref.startswith("refs/heads/"):
                return ref[11:]

        # GitLab CI
        if branch := os.environ.get("CI_COMMIT_BRANCH"):
            return branch

        return None

    def _get_latest_tag(self, repo: Repo) -> str | None:
        """Get the most recent semantic version tag that's an ancestor of HEAD."""
        version_pattern = re.compile(r"^\d+\.\d+\.\d+$")
        tags = sorted(
            repo.tags, key=lambda t: t.commit.committed_datetime, reverse=True
        )

        for tag in tags:
            if version_pattern.match(tag.name) and repo.is_ancestor(
                tag.commit, repo.head.commit
            ):
                return tag.name

        return None

    def _get_distance_from_tag(self, repo: Repo, tag: str) -> int:
        """Get number of commits since the given tag."""
        tag_commit = repo.tags[tag].commit
        commits = list(repo.iter_commits(f"{tag_commit.hexsha}..HEAD"))
        return len(commits)

    def _guess_next_version(self, version_number: str) -> str:
        """Guess next version by incrementing minor version."""
        base_version = version_number.lstrip("v")
        parts = base_version.split(".")

        major = int(parts[0])
        minor = int(parts[1])

        minor += 1
        return f"{major}.{minor}.0"


def get_version() -> str:
    """Entry point for hatch version source."""
    version = DBOSVersionSource(root=".", config={}).get_version_data()["version"]
    assert isinstance(version, str)
    return version
