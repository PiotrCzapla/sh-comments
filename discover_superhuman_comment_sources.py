#!/usr/bin/env python3

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from export_superhuman_comments_sqlite import find_databases, export_database


@dataclass(frozen=True)
class ProfileCandidate:
    source: str
    profile_dir: Path


def profile_dirs_under(root: Path) -> list[Path]:
    if not root.exists():
        return []

    candidates: list[Path] = []
    direct_names = {"Default", "Guest Profile", "System Profile"}
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        if child.name in direct_names or child.name.startswith("Profile "):
            candidates.append(child)
    return candidates


def discover_profile_candidates() -> list[ProfileCandidate]:
    home = Path.home()
    candidates: list[ProfileCandidate] = []

    chrome_root = home / "Library/Application Support/Google/Chrome"
    for profile in profile_dirs_under(chrome_root):
        candidates.append(ProfileCandidate(source="chrome", profile_dir=profile))

    arc_root = home / "Library/Application Support/Arc/User Data"
    for profile in profile_dirs_under(arc_root):
        candidates.append(ProfileCandidate(source="arc", profile_dir=profile))

    brave_root = home / "Library/Application Support/BraveSoftware/Brave-Browser"
    for profile in profile_dirs_under(brave_root):
        candidates.append(ProfileCandidate(source="brave", profile_dir=profile))

    edge_root = home / "Library/Application Support/Microsoft Edge"
    for profile in profile_dirs_under(edge_root):
        candidates.append(ProfileCandidate(source="edge", profile_dir=profile))

    chromium_root = home / "Library/Application Support/Chromium"
    for profile in profile_dirs_under(chromium_root):
        candidates.append(ProfileCandidate(source="chromium", profile_dir=profile))

    superhuman_app = home / "Library/Application Support/Superhuman"
    if superhuman_app.exists():
        candidates.append(ProfileCandidate(source="superhuman-app", profile_dir=superhuman_app))

    return candidates


def summarize_candidate(candidate: ProfileCandidate) -> dict:
    dbs = find_databases(candidate.profile_dir)
    if not dbs:
        indexeddb_dir = candidate.profile_dir / "IndexedDB"
        return {
            "source": candidate.source,
            "profileDir": str(candidate.profile_dir),
            "databaseCount": 0,
            "accounts": [],
            "commentsFound": 0,
            "threadsFound": 0,
            "latestCommentAt": "",
            "hasMailSuperhumanIndexedDB": (indexeddb_dir / "https_mail.superhuman.com_0.indexeddb.leveldb").exists(),
        }

    all_comments: list[dict] = []
    for db in dbs:
        all_comments.extend(export_database(db))

    return {
        "source": candidate.source,
        "profileDir": str(candidate.profile_dir),
        "databaseCount": len(dbs),
        "accounts": sorted({db.logical_name for db in dbs}),
        "commentsFound": len({row["commentId"] for row in all_comments}),
        "threadsFound": len({row["threadId"] for row in all_comments}),
        "latestCommentAt": max((row["createdAt"] for row in all_comments), default=""),
        "hasMailSuperhumanIndexedDB": (candidate.profile_dir / "IndexedDB" / "https_mail.superhuman.com_0.indexeddb.leveldb").exists(),
    }


def main() -> int:
    results = [summarize_candidate(candidate) for candidate in discover_profile_candidates()]
    ranked = sorted(
        results,
        key=lambda row: (
            row["commentsFound"],
            row["threadsFound"],
            row["latestCommentAt"],
            row["source"],
            row["profileDir"],
        ),
        reverse=True,
    )

    print(json.dumps({"results": ranked}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
