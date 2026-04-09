#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare two Superhuman comment export JSON files using stable "
            "(containerId, commentId) keys."
        )
    )
    parser.add_argument("left", type=Path, help="Path to the first export JSON")
    parser.add_argument("right", type=Path, help="Path to the second export JSON")
    parser.add_argument(
        "--left-name",
        default="left",
        help="Label for the first export in output",
    )
    parser.add_argument(
        "--right-name",
        default="right",
        help="Label for the second export in output",
    )
    parser.add_argument(
        "--cutoff",
        default="",
        help="Optional YYYY-MM-DD cutoff to test for a clean historical gap",
    )
    parser.add_argument(
        "--author",
        default="",
        help="Optional sharedBy/sharedByName email/name filter",
    )
    return parser.parse_args()


def load_comments(path: Path) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    comments = payload.get("comments") or []
    if not isinstance(comments, list):
        raise ValueError(f"{path} does not contain a comments list")
    return comments


def stable_key(comment: dict) -> tuple[str, str]:
    return (comment.get("containerId") or "", comment.get("commentId") or "")


def month_histogram(comments: Iterable[dict]) -> dict[str, int]:
    counter = Counter()
    for comment in comments:
        created_at = comment.get("createdAt") or ""
        if created_at:
            counter[created_at[:7]] += 1
    return dict(sorted(counter.items()))


def date_range(comments: Iterable[dict]) -> dict[str, str]:
    timestamps = sorted(
        comment.get("createdAt") or ""
        for comment in comments
        if comment.get("createdAt")
    )
    if not timestamps:
        return {"first": "", "last": ""}
    return {"first": timestamps[0], "last": timestamps[-1]}


def summarize_samples(comments: list[dict], limit: int = 5) -> list[dict]:
    items = []
    for comment in sorted(
        comments,
        key=lambda row: (row.get("createdAt") or "", row.get("commentId") or ""),
    )[:limit]:
        items.append(
            {
                "createdAt": comment.get("createdAt") or "",
                "sharedBy": comment.get("sharedBy") or "",
                "sharedByName": comment.get("sharedByName") or "",
                "threadSubject": comment.get("threadSubject") or "",
                "containerId": comment.get("containerId") or "",
                "commentId": comment.get("commentId") or "",
            }
        )
    return items


def apply_author_filter(comments: list[dict], author: str) -> list[dict]:
    if not author:
        return comments

    target = author.casefold()
    return [
        comment
        for comment in comments
        if (comment.get("sharedBy") or "").casefold() == target
        or (comment.get("sharedByName") or "").casefold() == target
    ]


def build_cutoff_summary(comments: list[dict], cutoff: str) -> dict[str, object]:
    if not cutoff:
        return {}

    before = [c for c in comments if (c.get("createdAt") or "")[:10] < cutoff]
    on_or_after = [c for c in comments if (c.get("createdAt") or "")[:10] >= cutoff]
    return {
        "cutoff": cutoff,
        "beforeCutoff": len(before),
        "onOrAfterCutoff": len(on_or_after),
        "cleanCutoff": len(on_or_after) == 0,
        "beforeCutoffRange": date_range(before),
        "onOrAfterCutoffRange": date_range(on_or_after),
    }


def compare(
    *,
    left_comments: list[dict],
    right_comments: list[dict],
    cutoff: str,
) -> dict[str, object]:
    left_by_key = {stable_key(comment): comment for comment in left_comments}
    right_by_key = {stable_key(comment): comment for comment in right_comments}

    left_only = [comment for key, comment in left_by_key.items() if key not in right_by_key]
    right_only = [comment for key, comment in right_by_key.items() if key not in left_by_key]

    left_containers = {comment.get("containerId") or "" for comment in left_comments}
    right_containers = {comment.get("containerId") or "" for comment in right_comments}
    shared_containers = left_containers & right_containers

    left_on_shared = [
        comment for comment in left_comments if (comment.get("containerId") or "") in shared_containers
    ]
    right_on_shared = [
        comment for comment in right_comments if (comment.get("containerId") or "") in shared_containers
    ]

    right_shared_by_key = {stable_key(comment): comment for comment in right_on_shared}
    left_shared_by_key = {stable_key(comment): comment for comment in left_on_shared}

    left_only_on_shared = [
        comment
        for key, comment in left_shared_by_key.items()
        if key not in right_shared_by_key
    ]
    right_only_on_shared = [
        comment
        for key, comment in right_shared_by_key.items()
        if key not in left_shared_by_key
    ]

    first_right_comment = min(
        (comment.get("createdAt") or "" for comment in right_comments if comment.get("createdAt")),
        default="",
    )
    left_only_on_or_after_first_right = [
        comment
        for comment in left_only
        if first_right_comment and (comment.get("createdAt") or "") >= first_right_comment
    ]

    return {
        "leftTotal": len(left_comments),
        "rightTotal": len(right_comments),
        "leftOnlyTotal": len(left_only),
        "rightOnlyTotal": len(right_only),
        "leftOnlyRange": date_range(left_only),
        "rightOnlyRange": date_range(right_only),
        "leftOnlyByMonth": month_histogram(left_only),
        "rightOnlyByMonth": month_histogram(right_only),
        "leftOnlyCutoff": build_cutoff_summary(left_only, cutoff),
        "rightOnlyCutoff": build_cutoff_summary(right_only, cutoff),
        "firstRightCommentAt": first_right_comment,
        "leftOnlyBeforeFirstRight": len(
            [
                comment
                for comment in left_only
                if first_right_comment and (comment.get("createdAt") or "") < first_right_comment
            ]
        ),
        "leftOnlyOnOrAfterFirstRight": len(left_only_on_or_after_first_right),
        "cleanCutoffAtFirstRight": len(left_only_on_or_after_first_right) == 0,
        "sharedContainers": len(shared_containers),
        "leftCommentsOnSharedContainers": len(left_on_shared),
        "rightCommentsOnSharedContainers": len(right_on_shared),
        "leftOnlyOnSharedContainers": len(left_only_on_shared),
        "rightOnlyOnSharedContainers": len(right_only_on_shared),
        "leftOnlyOnSharedRange": date_range(left_only_on_shared),
        "rightOnlyOnSharedRange": date_range(right_only_on_shared),
        "leftOnlyOnSharedByMonth": month_histogram(left_only_on_shared),
        "rightOnlyOnSharedByMonth": month_histogram(right_only_on_shared),
        "leftOnlyOnSharedCutoff": build_cutoff_summary(left_only_on_shared, cutoff),
        "rightOnlyOnSharedCutoff": build_cutoff_summary(right_only_on_shared, cutoff),
        "leftOnlySamples": summarize_samples(left_only),
        "rightOnlySamples": summarize_samples(right_only),
        "leftOnlyOnSharedSamples": summarize_samples(left_only_on_shared),
        "rightOnlyOnSharedSamples": summarize_samples(right_only_on_shared),
    }


def main() -> None:
    args = parse_args()

    left_comments = apply_author_filter(load_comments(args.left), args.author)
    right_comments = apply_author_filter(load_comments(args.right), args.author)

    result = {
        "left": {
            "name": args.left_name,
            "path": str(args.left.resolve()),
        },
        "right": {
            "name": args.right_name,
            "path": str(args.right.resolve()),
        },
        "authorFilter": args.author,
        "comparison": compare(
            left_comments=left_comments,
            right_comments=right_comments,
            cutoff=args.cutoff,
        ),
    }

    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
