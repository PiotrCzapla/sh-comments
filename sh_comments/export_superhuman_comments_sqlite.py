#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from shutil import copyfileobj

SQLITE_MAGIC = b"SQLite format 3\x00"
WRAPPER_HEADER_SIZE = 4096
DB_NAME_RE = re.compile(rb"/([^/\x00]+\.sqlite3)")


class TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in {"br", "p", "div", "li"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        self.parts.append(data)

    def text(self) -> str:
        joined = "".join(self.parts)
        lines = [line.strip() for line in joined.splitlines()]
        return "\n".join(line for line in lines if line).strip()


def html_to_text(value: str) -> str:
    parser = TextExtractor()
    parser.feed(value or "")
    parser.close()
    return unescape(parser.text())


@dataclass
class DatabaseCandidate:
    logical_name: str
    source_file: Path


def default_profile_dir() -> Path:
    env_profile = Path.home() / "Library/Application Support/Google/Chrome/Default"
    return Path(str(env_profile))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Superhuman shared-thread comments from Chrome's extension-backed SQLite storage."
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=str(Path.home() / "Downloads"),
        help="Directory where JSON and CSV exports should be written. Defaults to ~/Downloads.",
    )
    parser.add_argument(
        "--profile-dir",
        default=str(default_profile_dir()),
        help="Chrome profile directory. Defaults to ~/Library/Application Support/Google/Chrome/Default.",
    )
    parser.add_argument(
        "--account",
        action="append",
        default=[],
        help="Only export account databases matching the provided logical SQLite filename, for example pc@answer.ai.sqlite3.",
    )
    return parser.parse_args()


def find_databases(profile_dir: Path) -> list[DatabaseCandidate]:
    file_system_dir = profile_dir / "File System"
    candidates: list[DatabaseCandidate] = []
    if not file_system_dir.exists():
        return candidates

    for source_file in sorted(file_system_dir.glob("[0-9][0-9][0-9]/t/00/*")):
        try:
            with source_file.open("rb") as handle:
                wrapper_header = handle.read(WRAPPER_HEADER_SIZE)
                magic = handle.read(len(SQLITE_MAGIC))
        except OSError:
            continue

        if magic != SQLITE_MAGIC:
            continue

        match = DB_NAME_RE.search(wrapper_header)
        if not match:
            continue

        logical_name = match.group(1).decode("utf-8", "replace")
        candidates.append(DatabaseCandidate(logical_name=logical_name, source_file=source_file))

    return candidates


def copy_wrapped_sqlite(source_file: Path) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="superhuman-comments-"))
    temp_db = temp_dir / f"{source_file.name}.sqlite3"
    with source_file.open("rb") as source, temp_db.open("wb") as dest:
        source.seek(WRAPPER_HEADER_SIZE)
        copyfileobj(source, dest)
    return temp_db


def extract_thread_subject(thread_json: str) -> str:
    if not thread_json:
        return ""

    try:
        thread = json.loads(thread_json)
    except json.JSONDecodeError:
        return ""

    messages = thread.get("messages") or []
    if isinstance(messages, list):
        for message in messages:
            if isinstance(message, dict) and message.get("subject"):
                return str(message["subject"])
    return ""


def extract_comments_from_container(
    *,
    logical_name: str,
    source_file: Path,
    thread_id: str,
    subject: str,
    team_id: str,
    container_id: str,
    container: dict,
) -> list[dict]:
    comments: list[dict] = []
    messages = container.get("messages") or {}
    if not isinstance(messages, dict):
        return comments

    for message_key, message in messages.items():
        if not isinstance(message, dict):
            continue

        comment = message.get("comment")
        if not isinstance(comment, dict):
            continue

        comment_id = comment.get("id") or message_key
        body_html = comment.get("body") or ""
        sharing = message.get("sharing") or {}
        mentions = message.get("mentions") or []

        comments.append(
            {
                "accountDb": logical_name,
                "sourceFile": str(source_file),
                "threadId": thread_id,
                "threadSubject": subject,
                "teamId": team_id,
                "containerId": container_id,
                "containerPath": container.get("path") or "",
                "containerLink": container.get("link") or "",
                "commentId": comment_id,
                "messageKey": message_key,
                "contentType": comment.get("contentType") or "",
                "bodyHtml": body_html,
                "bodyText": html_to_text(body_html),
                "createdAt": comment.get("createdAt") or "",
                "clientCreatedAt": comment.get("clientCreatedAt") or "",
                "sharedAt": sharing.get("sharedAt") or "",
                "sharedBy": sharing.get("by") or "",
                "sharedByName": sharing.get("name") or "",
                "accessRole": sharing.get("accessRole") or "",
                "mentions": json.dumps(mentions, ensure_ascii=True),
                "raw": json.dumps(message, ensure_ascii=True),
            }
        )

    return comments


def extract_comments_from_row(
    *,
    logical_name: str,
    source_file: Path,
    thread_id: str,
    thread_json: str,
    superhuman_data: str,
) -> list[dict]:
    if not superhuman_data:
        return []

    try:
        shared_data = json.loads(superhuman_data)
    except json.JSONDecodeError:
        return []

    subject = extract_thread_subject(thread_json)
    comments: list[dict] = []
    teams = shared_data.get("teams") or {}
    if not isinstance(teams, dict):
        return comments

    for team_id, team in teams.items():
        if not isinstance(team, dict):
            continue
        containers = team.get("containers") or {}
        if not isinstance(containers, dict):
            continue
        for container_id, container in containers.items():
            if not isinstance(container, dict):
                continue
            comments.extend(
                extract_comments_from_container(
                    logical_name=logical_name,
                    source_file=source_file,
                    thread_id=thread_id,
                    subject=subject,
                    team_id=team_id,
                    container_id=container_id,
                    container=container,
                )
            )

    return comments


def export_database(candidate: DatabaseCandidate) -> list[dict]:
    temp_db = copy_wrapped_sqlite(candidate.source_file)
    connection = sqlite3.connect(str(temp_db))
    connection.row_factory = sqlite3.Row

    try:
        connection.execute("PRAGMA quick_check;").fetchone()
        rows = connection.execute(
            """
            SELECT thread_id, json, superhuman_data
            FROM threads
            WHERE superhuman_data LIKE '%"comment"%'
            """
        ).fetchall()
    finally:
        connection.close()

    comments: list[dict] = []
    for row in rows:
        comments.extend(
            extract_comments_from_row(
                logical_name=candidate.logical_name,
                source_file=candidate.source_file,
                thread_id=row["thread_id"],
                thread_json=row["json"] or "",
                superhuman_data=row["superhuman_data"] or "",
            )
        )

    return comments


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict]) -> None:
    fieldnames = [
        "accountDb",
        "sourceFile",
        "threadId",
        "threadSubject",
        "teamId",
        "containerId",
        "containerPath",
        "containerLink",
        "commentId",
        "messageKey",
        "contentType",
        "bodyText",
        "bodyHtml",
        "createdAt",
        "clientCreatedAt",
        "sharedAt",
        "sharedBy",
        "sharedByName",
        "accessRole",
        "mentions",
        "raw",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    profile_dir = Path(args.profile_dir).expanduser()
    output_dir = Path(args.output_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)

    candidates = find_databases(profile_dir)
    if args.account:
        requested = set(args.account)
        candidates = [candidate for candidate in candidates if candidate.logical_name in requested]

    if not candidates:
        print(
            json.dumps(
                {
                    "error": "No wrapped Superhuman SQLite databases were found.",
                    "profileDir": str(profile_dir),
                }
            ),
            file=sys.stderr,
        )
        return 1

    all_comments: list[dict] = []
    accounts_scanned: list[str] = []
    for candidate in candidates:
        accounts_scanned.append(candidate.logical_name)
        all_comments.extend(export_database(candidate))

    seen_keys: set[str] = set()
    unique_comments: list[dict] = []
    for comment in sorted(
        all_comments,
        key=lambda row: (
            row.get("createdAt", ""),
            row.get("commentId", ""),
            row.get("accountDb", ""),
        ),
    ):
        key = "|".join(
            [
                comment.get("accountDb", ""),
                comment.get("commentId", ""),
                comment.get("createdAt", ""),
            ]
        )
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique_comments.append(comment)

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    json_path = output_dir / f"superhuman-comments-sqlite-{stamp}.json"
    csv_path = output_dir / f"superhuman-comments-sqlite-{stamp}.csv"

    summary = {
        "exportedAt": datetime.now(timezone.utc).isoformat(),
        "profileDir": str(profile_dir),
        "accountsScanned": accounts_scanned,
        "commentsFound": len(unique_comments),
        "jsonPath": str(json_path),
        "csvPath": str(csv_path),
    }

    write_json(json_path, {"summary": summary, "comments": unique_comments})
    write_csv(csv_path, unique_comments)
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
