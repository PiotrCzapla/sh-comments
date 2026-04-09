#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any


SNIPPET_ACTION = b'"action":"snippet"'
OWNER_PATTERN = re.compile(r'"name":"([^"]+)"},"draft":\{')


class TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
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


@dataclass(frozen=True)
class SnippetHit:
    source_root: str
    source_file: str
    owner_name: str
    draft: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract Superhuman snippet drafts from File System blob storage and "
            "write deduplicated JSON and CSV exports."
        )
    )
    parser.add_argument(
        "roots",
        nargs="*",
        help=(
            "One or more Superhuman File System roots. Defaults to "
            "'./Lukes File System' and './Scott File System' when present."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="exports",
        help="Directory where exports should be written. Defaults to ./exports.",
    )
    parser.add_argument(
        "--prefix",
        default="superhuman_snippets",
        help="Filename prefix for written exports. Defaults to superhuman_snippets.",
    )
    return parser.parse_args()


def default_roots(cwd: Path) -> list[Path]:
    candidates = [
        cwd / "Lukes File System",
        cwd / "Scott File System",
    ]
    return [path for path in candidates if path.exists()]


def iter_blob_files(root: Path) -> list[Path]:
    blobs = [
        path
        for path in root.rglob("*")
        if path.is_file() and "/t/" in path.as_posix() and path.name.isdigit()
    ]
    return sorted(blobs)


def find_json_start(text: str, match_index: int) -> int:
    window_start = max(0, match_index - 32_000)
    window = text[window_start:match_index]

    candidates = []
    cursor = window.find('{"schemaVersion":')
    while cursor != -1:
        candidates.append(window_start + cursor)
        cursor = window.find('{"schemaVersion":', cursor + 1)

    draft_candidates = []
    cursor = window.find('"draft":{"schemaVersion":')
    while cursor != -1:
        draft_candidates.append(window_start + cursor + len('"draft":'))
        cursor = window.find('"draft":{"schemaVersion":', cursor + 1)

    all_candidates = candidates + draft_candidates
    if not all_candidates:
        return -1
    return max(all_candidates)


def extract_balanced_json(text: str, start_index: int) -> str:
    depth = 0
    in_string = False
    escaped = False

    for index in range(start_index, len(text)):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start_index : index + 1]

    raise ValueError("Could not find matching JSON object boundary")


def infer_owner_name(text: str, draft_start: int) -> str:
    context = text[max(0, draft_start - 1_500) : draft_start + len('{"schemaVersion":')]
    matches = list(OWNER_PATTERN.finditer(context))
    if not matches:
        return ""
    return matches[-1].group(1)


def load_candidate_text(source_file: Path) -> str:
    try:
        result = subprocess.run(
            ["strings", "-n", "12", str(source_file)],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout
    except (FileNotFoundError, subprocess.CalledProcessError):
        try:
            blob = source_file.read_bytes()
        except OSError:
            return ""
        return blob.decode("latin-1")


def extract_snippets_from_file(source_root: Path, source_file: Path) -> list[SnippetHit]:
    text = load_candidate_text(source_file)
    if not text:
        return []
    if SNIPPET_ACTION.decode("latin-1") not in text:
        return []

    hits: list[SnippetHit] = []
    search_start = 0

    while True:
        match_index = text.find(SNIPPET_ACTION.decode("latin-1"), search_start)
        if match_index == -1:
            break

        draft_start = find_json_start(text, match_index)
        search_start = match_index + len(SNIPPET_ACTION)
        if draft_start == -1:
            continue

        try:
            draft_json = extract_balanced_json(text, draft_start)
            draft = json.loads(draft_json)
        except (ValueError, json.JSONDecodeError):
            continue

        if not isinstance(draft, dict):
            continue
        if draft.get("action") != "snippet":
            continue
        draft_id = draft.get("id")
        if not isinstance(draft_id, str) or not draft_id.startswith("draft"):
            continue

        hits.append(
            SnippetHit(
                source_root=source_root.name,
                source_file=str(source_file),
                owner_name=infer_owner_name(text, draft_start),
                draft=draft,
            )
        )

    return hits


def timestamp_from_millis(value: Any) -> str:
    if not isinstance(value, int):
        return ""
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


def merge_hits(hits: list[SnippetHit]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    for hit in hits:
        draft = hit.draft
        draft_id = str(draft.get("id", ""))
        if not draft_id:
            continue

        row = merged.setdefault(
            draft_id,
            {
                "draftId": draft_id,
                "name": draft.get("name", ""),
                "ownerName": hit.owner_name,
                "action": draft.get("action", ""),
                "subject": draft.get("subject", ""),
                "bodyHtml": draft.get("body", ""),
                "bodyText": html_to_text(str(draft.get("body", ""))),
                "snippetPreview": draft.get("snippet", ""),
                "clientCreatedAt": draft.get("clientCreatedAt", ""),
                "clientCreatedAtIso": timestamp_from_millis(draft.get("clientCreatedAt")),
                "date": draft.get("date", ""),
                "threadId": draft.get("threadId", ""),
                "fromEmail": (draft.get("from") or {}).get("email", "") if isinstance(draft.get("from"), dict) else "",
                "labelIds": sorted(str(label) for label in (draft.get("labelIds") or []) if isinstance(label, str)),
                "sourceRoots": set(),
                "sourceFiles": set(),
                "seenCount": 0,
            },
        )

        row["seenCount"] += 1
        row["sourceRoots"].add(hit.source_root)
        row["sourceFiles"].add(hit.source_file)

        if not row["ownerName"] and hit.owner_name:
            row["ownerName"] = hit.owner_name
        if not row["bodyHtml"] and draft.get("body"):
            row["bodyHtml"] = draft.get("body", "")
            row["bodyText"] = html_to_text(str(draft.get("body", "")))
        if not row["snippetPreview"] and draft.get("snippet"):
            row["snippetPreview"] = draft.get("snippet", "")

    rows: list[dict[str, Any]] = []
    for row in merged.values():
        row["sourceRoots"] = sorted(row["sourceRoots"])
        row["sourceFiles"] = sorted(row["sourceFiles"])
        rows.append(row)

    rows.sort(
        key=lambda item: (
            item["ownerName"],
            item["name"],
            item["clientCreatedAtIso"],
            item["draftId"],
        )
    )
    return rows


def write_json(path: Path, rows: list[dict[str, Any]]) -> None:
    payload = {
        "snippets": rows,
        "count": len(rows),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "draftId",
        "name",
        "ownerName",
        "action",
        "subject",
        "snippetPreview",
        "bodyText",
        "clientCreatedAt",
        "clientCreatedAtIso",
        "date",
        "threadId",
        "fromEmail",
        "labelIds",
        "sourceRoots",
        "seenCount",
        "sourceFiles",
    ]

    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "draftId": row["draftId"],
                    "name": row["name"],
                    "ownerName": row["ownerName"],
                    "action": row["action"],
                    "subject": row["subject"],
                    "snippetPreview": row["snippetPreview"],
                    "bodyText": row["bodyText"],
                    "clientCreatedAt": row["clientCreatedAt"],
                    "clientCreatedAtIso": row["clientCreatedAtIso"],
                    "date": row["date"],
                    "threadId": row["threadId"],
                    "fromEmail": row["fromEmail"],
                    "labelIds": "; ".join(row["labelIds"]),
                    "sourceRoots": "; ".join(row["sourceRoots"]),
                    "seenCount": row["seenCount"],
                    "sourceFiles": "; ".join(row["sourceFiles"]),
                }
            )


def collect_hits(roots: list[Path]) -> list[SnippetHit]:
    hits: list[SnippetHit] = []
    for root in roots:
        for source_file in iter_blob_files(root):
            hits.extend(extract_snippets_from_file(root, source_file))
    return hits


def main() -> int:
    args = parse_args()
    cwd = Path.cwd()
    roots = [Path(root) for root in args.roots] if args.roots else default_roots(cwd)
    if not roots:
        raise SystemExit("No File System roots were provided and default roots were not found.")

    missing = [str(root) for root in roots if not root.exists()]
    if missing:
        raise SystemExit(f"Missing roots: {', '.join(missing)}")

    hits = collect_hits(roots)
    rows = merge_hits(hits)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{args.prefix}.json"
    csv_path = output_dir / f"{args.prefix}.csv"

    write_json(json_path, rows)
    write_csv(csv_path, rows)

    owners = sorted({row["ownerName"] for row in rows if row["ownerName"]})
    print(
        json.dumps(
            {
                "roots": [str(root) for root in roots],
                "snippetCount": len(rows),
                "owners": owners,
                "jsonPath": str(json_path),
                "csvPath": str(csv_path),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
