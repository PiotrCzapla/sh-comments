#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import contextlib
import json
import os
import sqlite3
import socket
import struct
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .export_superhuman_comments_sqlite import (
    DatabaseCandidate,
    copy_wrapped_sqlite,
    html_to_text,
    write_json,
)
from .superhuman_live_cookie_probe import (
    ApiError,
    CookieRecord,
    SUPERHUMAN_VERSION,
    USER_AGENT,
    build_cookie_header,
    json_safe_error,
    load_superhuman_cookies,
)


@dataclass(frozen=True)
class AccountContext:
    logical_name: str
    email: str
    google_id: str
    team_id: str
    team_name: str
    members_count: int
    source_file: Path | None


@dataclass(frozen=True)
class CDPLiveContext:
    email: str
    google_id: str
    team_id: str
    superhuman_id_token: str
    gmail_access_token: str
    session_id: str
    device_id: str
    version: str
    user_external_id: str


def default_profile_dir() -> Path:
    return Path.home() / "Library/Application Support/Google/Chrome/Default"


def default_data_dir() -> Path:
    return Path.home() / "Library/Application Support/Superhuman"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Superhuman comments using either a live authenticated session "
            "(manual cookie header or idToken) or cache-backed thread payloads."
        )
    )
    parser.add_argument(
        "--data-dir",
        default=str(default_data_dir()),
        help=(
            "Superhuman data dir or extracted File System root. "
            "Defaults to ~/Library/Application Support/Superhuman."
        ),
    )
    parser.add_argument(
        "--profile-dir",
        default=str(default_profile_dir()),
        help=(
            "Chrome profile dir used only when loading cookies from Chrome directly. "
            "Defaults to ~/Library/Application Support/Google/Chrome/Default."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="exports",
        help="Directory where JSON and CSV exports should be written. Defaults to ./exports.",
    )
    parser.add_argument(
        "--prefix",
        default="superhuman_live_comments",
        help="Filename prefix for exports. Defaults to superhuman_live_comments.",
    )
    parser.add_argument(
        "--account-email",
        help="Specific account email to export, for example scott@tryvirgil.com.",
    )
    parser.add_argument(
        "--cookie-header",
        help=(
            "Raw Cookie header copied from a logged-in Superhuman request. "
            "If provided, the script skips Chrome Keychain cookie decryption."
        ),
    )
    parser.add_argument(
        "--cookie",
        action="append",
        default=[],
        help="Individual cookie in NAME=VALUE form. Can be passed multiple times.",
    )
    parser.add_argument(
        "--cdp-url",
        help=(
            "Chrome DevTools Protocol HTTP endpoint, for example http://localhost:9222. "
            "If provided, the script can pull live Superhuman cookies directly from the browser."
        ),
    )
    parser.add_argument(
        "--gmail-access-token",
        help=(
            "Google OAuth bearer token for Gmail API thread enumeration. "
            "If omitted and --cdp-url is provided, the script will try to capture it from the live browser."
        ),
    )
    parser.add_argument(
        "--id-token",
        help=(
            "Bearer token from an authenticated Superhuman request. "
            "If provided, the script skips sessions.getCsrfToken/getTokens."
        ),
    )
    parser.add_argument(
        "--google-id",
        help=(
            "Provider/Google user ID. Usually auto-discovered from the local Superhuman "
            "settings store, so only needed when local discovery is unavailable."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="HTTP timeout in seconds for live requests. Defaults to 15.",
    )
    parser.add_argument(
        "--threads-page-size",
        type=int,
        default=100,
        help="Page size for live userdata.getThreads pagination. Defaults to 100.",
    )
    parser.add_argument(
        "--gmail-page-size",
        type=int,
        default=500,
        help="Page size for Gmail API thread enumeration. Defaults to 500.",
    )
    parser.add_argument(
        "--max-live-threads",
        type=int,
        help="Optional cap on the number of live-enumerated thread ids to crawl.",
    )
    parser.add_argument(
        "--cache-only",
        action="store_true",
        help="Never attempt live auth or network calls; export only from cached thread payloads.",
    )
    parser.add_argument(
        "--live-only",
        action="store_true",
        help=(
            "Use only live enumeration and live thread reads. Requires CDP or explicit live tokens. "
            "Does not depend on local Superhuman cache."
        ),
    )
    return parser.parse_args()


def resolve_file_system_dir(data_dir: Path) -> Path:
    expanded = data_dir.expanduser()
    if (expanded / "File System").exists():
        return expanded / "File System"

    if expanded.exists() and any(child.is_dir() and child.name.isdigit() and len(child.name) == 3 for child in expanded.iterdir()):
        return expanded

    raise FileNotFoundError(
        f"Could not find a Superhuman File System directory under {expanded}"
    )


def find_databases_in_dir(data_dir: Path) -> list[DatabaseCandidate]:
    file_system_dir = resolve_file_system_dir(data_dir)
    candidates: list[DatabaseCandidate] = []
    for source_file in sorted(file_system_dir.glob("[0-9][0-9][0-9]/t/00/*")):
        if not source_file.is_file():
            continue
        try:
            with source_file.open("rb") as handle:
                wrapper_header = handle.read(4096)
                magic = handle.read(len(b"SQLite format 3\x00"))
        except OSError:
            continue

        if magic != b"SQLite format 3\x00":
            continue

        marker = b"/"
        suffix = b".sqlite3"
        start = wrapper_header.rfind(marker)
        end = wrapper_header.find(suffix, start)
        if start == -1 or end == -1:
            continue
        logical_name = wrapper_header[start + 1 : end + len(suffix)].decode("utf-8", "replace")
        candidates.append(DatabaseCandidate(logical_name=logical_name, source_file=source_file))
    return candidates


def read_general_json(candidate: DatabaseCandidate, key: str) -> dict[str, Any]:
    temp_db = copy_wrapped_sqlite(candidate.source_file)
    connection = sqlite3.connect(str(temp_db))
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            "SELECT json FROM general WHERE key = ?",
            (key,),
        ).fetchone()
    finally:
        connection.close()

    if not row or not row["json"]:
        return {}
    return json.loads(row["json"])


def load_commented_threads(candidate: DatabaseCandidate) -> list[dict[str, str]]:
    temp_db = copy_wrapped_sqlite(candidate.source_file)
    connection = sqlite3.connect(str(temp_db))
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT thread_id, json, superhuman_data
            FROM threads
            WHERE superhuman_data LIKE '%"comment"%'
            """
        ).fetchall()
    finally:
        connection.close()

    return [
        {
            "threadId": str(row["thread_id"] or ""),
            "threadJson": str(row["json"] or ""),
            "superhumanData": str(row["superhuman_data"] or ""),
        }
        for row in rows
    ]


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


def choose_account_context(data_dir: Path, account_email: str | None) -> tuple[AccountContext, list[dict[str, str]]]:
    candidates = [
        candidate
        for candidate in find_databases_in_dir(data_dir)
        if candidate.logical_name != "demo@superhuman.com.sqlite3"
    ]
    if not candidates:
        raise RuntimeError("No non-demo Superhuman account databases were found.")

    if account_email:
        matching = [
            candidate
            for candidate in candidates
            if candidate.logical_name == account_email or candidate.logical_name == f"{account_email}.sqlite3"
        ]
        if not matching:
            available = [candidate.logical_name for candidate in candidates]
            raise RuntimeError(
                f"Account {account_email!r} was not found. Available accounts: {available}"
            )
        candidates = matching

    candidate = candidates[0]
    settings = read_general_json(candidate, "settings")
    commented_threads = load_commented_threads(candidate)

    calendar_accounts = settings.get("calendarAccounts") or {}
    if not isinstance(calendar_accounts, dict) or not calendar_accounts:
        raise RuntimeError("Could not find calendarAccounts in Superhuman settings.")

    preferred_email = account_email or candidate.logical_name.removesuffix(".sqlite3")
    account_record = calendar_accounts.get(preferred_email)
    if not isinstance(account_record, dict):
        first_email, account_record = next(iter(calendar_accounts.items()))
        preferred_email = str(first_email)

    google_id = account_record.get("googleId") or account_record.get("userId")
    if not google_id:
        raise RuntimeError("Could not find the Google/provider user ID in Superhuman settings.")

    team_info = settings.get("teams") or {}
    members = team_info.get("members") or []

    return (
        AccountContext(
            logical_name=candidate.logical_name,
            email=str(preferred_email),
            google_id=str(google_id),
            team_id=str(team_info.get("teamId") or ""),
            team_name=str(team_info.get("name") or ""),
            members_count=len(members) if isinstance(members, list) else 0,
            source_file=candidate.source_file,
        ),
        commented_threads,
    )


def parse_cookie_header(cookie_header: str) -> list[CookieRecord]:
    cookies: list[CookieRecord] = []
    for pair in cookie_header.split(";"):
        if "=" not in pair:
            continue
        name, value = pair.split("=", 1)
        name = name.strip()
        value = value.strip()
        if not name:
            continue
        cookies.append(
            CookieRecord(
                host_key=".superhuman.com",
                name=name,
                value=value,
                path="/",
                is_secure=True,
            )
        )
    return cookies


def parse_cookie_pairs(cookie_pairs: list[str]) -> list[CookieRecord]:
    cookies: list[CookieRecord] = []
    for pair in cookie_pairs:
        if "=" not in pair:
            raise ValueError(f"Invalid --cookie value {pair!r}; expected NAME=VALUE")
        name, value = pair.split("=", 1)
        cookies.append(
            CookieRecord(
                host_key=".superhuman.com",
                name=name.strip(),
                value=value.strip(),
                path="/",
                is_secure=True,
            )
        )
    return cookies


def cdp_call(ws_url: str, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    parsed = urllib.parse.urlparse(ws_url)
    websocket_key = base64.b64encode(os.urandom(16)).decode()
    raw = socket.create_connection((parsed.hostname or "localhost", parsed.port or 80))
    request = (
        f"GET {parsed.path} HTTP/1.1\r\n"
        f"Host: {parsed.hostname}:{parsed.port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {websocket_key}\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n"
    )
    raw.sendall(request.encode())
    response = raw.recv(4096)
    if b"101" not in response.split(b"\r\n", 1)[0]:
        raise RuntimeError(response.decode("latin1", "replace"))

    def send(obj: dict[str, Any]) -> None:
        data = json.dumps(obj).encode()
        frame = bytearray([0x81])
        length = len(data)
        if length < 126:
            frame.append(0x80 | length)
        elif length < 65536:
            frame.append(0x80 | 126)
            frame.extend(struct.pack("!H", length))
        else:
            frame.append(0x80 | 127)
            frame.extend(struct.pack("!Q", length))
        mask = os.urandom(4)
        frame.extend(mask)
        frame.extend(bytes(byte ^ mask[index % 4] for index, byte in enumerate(data)))
        raw.sendall(frame)

    def recv_one() -> tuple[int, bytes]:
        header = raw.recv(2)
        if len(header) < 2:
            raise EOFError("CDP websocket closed unexpectedly")
        first, second = header
        opcode = first & 0x0F
        masked = (second >> 7) & 1
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", raw.recv(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", raw.recv(8))[0]
        mask = raw.recv(4) if masked else b""
        payload = bytearray()
        while len(payload) < length:
            payload.extend(raw.recv(length - len(payload)))
        if masked:
            payload = bytearray(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        return opcode, bytes(payload)

    message_id = 1
    send({"id": message_id, "method": method, "params": params or {}})
    try:
        while True:
            opcode, payload = recv_one()
            if opcode != 1:
                continue
            message = json.loads(payload.decode())
            if message.get("id") != message_id:
                continue
            if "error" in message:
                raise RuntimeError(message["error"])
            return message.get("result") or {}
    finally:
        raw.close()


def get_cdp_mail_page(cdp_url: str) -> dict[str, Any]:
    pages = json.load(urllib.request.urlopen(urllib.parse.urljoin(cdp_url, "/json/list")))
    page = next(
        (
            item
            for item in pages
            if item.get("type") == "page" and str(item.get("url", "")).startswith("https://mail.superhuman.com/")
        ),
        None,
    )
    if not page:
        raise RuntimeError("No mail.superhuman.com page was found at the provided CDP endpoint.")
    return page


def load_cdp_superhuman_cookies(cdp_url: str) -> tuple[str, list[CookieRecord]]:
    page = get_cdp_mail_page(cdp_url)

    result = cdp_call(
        str(page["webSocketDebuggerUrl"]),
        "Network.getCookies",
        {"urls": ["https://mail.superhuman.com", "https://accounts.superhuman.com"]},
    )
    cookies = [
        CookieRecord(
            host_key=str(cookie.get("domain") or ".superhuman.com"),
            name=str(cookie.get("name") or ""),
            value=str(cookie.get("value") or ""),
            path=str(cookie.get("path") or "/"),
            is_secure=bool(cookie.get("secure")),
        )
        for cookie in result.get("cookies") or []
        if "superhuman.com" in str(cookie.get("domain") or "")
    ]
    if not cookies:
        raise RuntimeError("No Superhuman cookies were found via CDP.")
    return (f"cdp:{cdp_url}", cookies)


def fetch_google_userinfo(access_token: str, timeout: float = 15.0) -> dict[str, Any]:
    request = urllib.request.Request(
        url="https://openidconnect.googleapis.com/v1/userinfo",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload if isinstance(payload, dict) else {}


def cdp_network_capture(cdp_url: str, *, reload_page: bool = True, duration_seconds: float = 8.0) -> list[dict[str, Any]]:
    page = get_cdp_mail_page(cdp_url)
    ws_url = str(page["webSocketDebuggerUrl"])
    parsed = urllib.parse.urlparse(ws_url)
    websocket_key = base64.b64encode(os.urandom(16)).decode()
    raw = socket.create_connection((parsed.hostname or "localhost", parsed.port or 80))
    request = (
        f"GET {parsed.path} HTTP/1.1\r\n"
        f"Host: {parsed.hostname}:{parsed.port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {websocket_key}\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n"
    )
    raw.sendall(request.encode())
    response = raw.recv(4096)
    if b"101" not in response.split(b"\r\n", 1)[0]:
        raise RuntimeError(response.decode("latin1", "replace"))
    raw.settimeout(0.5)
    message_id = 1

    def send(obj: dict[str, Any]) -> None:
        data = json.dumps(obj).encode()
        frame = bytearray([0x81])
        length = len(data)
        if length < 126:
            frame.append(0x80 | length)
        elif length < 65536:
            frame.append(0x80 | 126)
            frame.extend(struct.pack("!H", length))
        else:
            frame.append(0x80 | 127)
            frame.extend(struct.pack("!Q", length))
        mask = os.urandom(4)
        frame.extend(mask)
        frame.extend(bytes(byte ^ mask[index % 4] for index, byte in enumerate(data)))
        raw.sendall(frame)

    def recv_one() -> tuple[int, bytes]:
        header = raw.recv(2)
        if len(header) < 2:
            raise EOFError("CDP websocket closed unexpectedly")
        first, second = header
        opcode = first & 0x0F
        masked = (second >> 7) & 1
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", raw.recv(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", raw.recv(8))[0]
        mask = raw.recv(4) if masked else b""
        payload = bytearray()
        while len(payload) < length:
            payload.extend(raw.recv(length - len(payload)))
        if masked:
            payload = bytearray(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        return opcode, bytes(payload)

    def call(method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        nonlocal message_id
        current_id = message_id
        message_id += 1
        send({"id": current_id, "method": method, "params": params or {}})
        while True:
            try:
                opcode, payload = recv_one()
            except TimeoutError:
                continue
            if opcode != 1:
                continue
            msg = json.loads(payload.decode())
            if msg.get("id") != current_id:
                continue
            if "error" in msg:
                raise RuntimeError(msg["error"])
            return msg.get("result") or {}

    try:
        call("Network.enable", {"includeExtraInfo": True})
        call("Page.enable", {})
        if reload_page:
            call("Page.reload", {"ignoreCache": True})
        deadline = datetime.now(timezone.utc).timestamp() + duration_seconds
        events: list[dict[str, Any]] = []
        while datetime.now(timezone.utc).timestamp() < deadline:
            try:
                opcode, payload = recv_one()
            except TimeoutError:
                continue
            if opcode != 1:
                continue
            with contextlib.suppress(json.JSONDecodeError):
                events.append(json.loads(payload.decode()))
        return events
    finally:
        raw.close()


def load_cdp_gmail_access_token(cdp_url: str) -> str:
    events = cdp_network_capture(cdp_url, reload_page=True, duration_seconds=8.0)
    requests: dict[str, dict[str, Any]] = {}
    for event in events:
        method = event.get("method")
        params = event.get("params") or {}
        if method == "Network.requestWillBeSent":
            request = params.get("request") or {}
            url = str(request.get("url") or "")
            if "content.googleapis.com/gmail/v1/users/me/threads" in url:
                requests[str(params.get("requestId"))] = {
                    "headers": request.get("headers") or {},
                    "url": url,
                }
        elif method == "Network.requestWillBeSentExtraInfo":
            request_id = str(params.get("requestId"))
            if request_id in requests:
                requests[request_id]["extraHeaders"] = params.get("headers") or {}

    for request in requests.values():
        for headers in (request.get("extraHeaders") or {}, request.get("headers") or {}):
            authorization = headers.get("authorization") or headers.get("Authorization")
            if isinstance(authorization, str) and authorization.startswith("Bearer "):
                return authorization.removeprefix("Bearer ").strip()

    raise RuntimeError("Could not capture a Gmail API bearer token from the live CDP session.")


class SuperhumanClient:
    def __init__(
        self,
        *,
        email: str,
        provider_id: str,
        cookies: list[CookieRecord],
        timeout: float,
        id_token: str | None = None,
        session_id: str | None = None,
        device_id: str | None = None,
        version: str | None = None,
        user_external_id: str | None = None,
    ) -> None:
        self.email = email
        self.provider_id = provider_id
        self.cookies = cookies
        self.timeout = timeout
        self.session_id = session_id or str(uuid.uuid4())
        self.device_id = device_id or str(uuid.uuid4())
        self.id_token = id_token
        self.version = version or SUPERHUMAN_VERSION
        self.user_external_id = user_external_id or ""

    def _request_json(
        self,
        *,
        base_url: str,
        path: str,
        endpoint: str,
        method: str = "GET",
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        require_auth: bool = False,
    ) -> dict[str, Any]:
        url = urllib.parse.urljoin(base_url, path)
        host = urllib.parse.urlparse(url).hostname or ""
        body_bytes = json.dumps(body).encode("utf-8") if body is not None else None

        request_headers = {
            "Accept": "application/json, text/plain, */*",
            "Cache-Control": "no-store",
            "Origin": "https://mail.superhuman.com",
            "Referer": "https://mail.superhuman.com/",
            "User-Agent": USER_AGENT,
            "x-superhuman-session-id": self.session_id,
            "x-superhuman-user-email": self.email,
            "x-superhuman-request-id": str(uuid.uuid4()),
            "x-superhuman-device-id": self.device_id,
            "x-superhuman-version": self.version,
        }
        if self.user_external_id:
            request_headers["x-superhuman-user-external-id"] = self.user_external_id

        cookie_header = build_cookie_header(self.cookies, host)
        if cookie_header:
            request_headers["Cookie"] = cookie_header

        if body_bytes is not None:
            request_headers["Content-Type"] = "application/json"

        if require_auth:
            if not self.id_token:
                raise RuntimeError(f"{endpoint} requires an ID token, but none is loaded")
            request_headers["Authorization"] = f"Bearer {self.id_token}"

        if headers:
            request_headers.update(headers)

        request = urllib.request.Request(
            url=url,
            data=body_bytes,
            headers=request_headers,
            method=method,
        )

        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                payload = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body_text = exc.read().decode("utf-8", "replace")
            raise ApiError(method=method, url=url, status=exc.code, body=body_text) from exc

        if not payload:
            return {}
        parsed = json.loads(payload)
        if not isinstance(parsed, dict):
            raise RuntimeError(f"{endpoint} returned a non-object JSON payload")
        return parsed

    def get_csrf_token(self) -> dict[str, Any]:
        return self._request_json(
            base_url="https://accounts.superhuman.com",
            path="/~backend/v3/sessions.getCsrfToken",
            endpoint="sessions.getCsrfToken",
        )

    def get_tokens(self, csrf_token: str) -> dict[str, Any]:
        response = self._request_json(
            base_url="https://accounts.superhuman.com",
            path="/~backend/v3/sessions.getTokens",
            endpoint="sessions.getTokens",
            method="POST",
            body={
                "emailAddress": self.email,
                "googleId": self.provider_id,
            },
            headers={"X-CSRF-Token": csrf_token},
        )
        auth_data = response.get("authData") or {}
        token = auth_data.get("idToken")
        if not isinstance(token, str) or not token:
            raise RuntimeError("sessions.getTokens did not return an idToken")
        self.id_token = token
        return response

    def read_user_data(self, relative_path: str) -> dict[str, Any]:
        full_path = f"users/{self.provider_id}/{relative_path}"
        return self._request_json(
            base_url="https://mail.superhuman.com",
            path="/~backend/v3/userdata.read",
            endpoint="userdata.read",
            method="POST",
            body={"reads": [{"path": full_path}], "pageSize": 200},
            require_auth=True,
        )

    def get_threads(self, *, limit: int, offset: int = 0) -> dict[str, Any]:
        return self._request_json(
            base_url="https://mail.superhuman.com",
            path="/~backend/v3/userdata.getThreads",
            endpoint="userdata.getThreads",
            method="POST",
            body={"offset": offset, "limit": limit},
            require_auth=True,
        )


def unwrap_userdata_value(response: dict[str, Any]) -> Any:
    results = response.get("results") or []
    if isinstance(results, list) and results:
        first = results[0]
        if isinstance(first, dict):
            return first.get("value")
    return None


def extract_comments_from_payload(
    *,
    payload: Any,
    logical_name: str,
    source_file: Path,
    thread_id: str,
    thread_subject: str,
    source: str,
) -> list[dict[str, str]]:
    comments: list[dict[str, str]] = []

    def walk(node: Any, *, team_id: str = "", container_path: str = "", container_link: str = "") -> None:
        if isinstance(node, list):
            for item in node:
                walk(item, team_id=team_id, container_path=container_path, container_link=container_link)
            return

        if not isinstance(node, dict):
            return

        next_team_id = team_id
        next_container_path = container_path
        next_container_link = container_link

        path_value = node.get("path")
        if isinstance(path_value, str) and path_value.startswith("teams/"):
            next_container_path = path_value
            parts = path_value.split("/")
            if len(parts) > 1:
                next_team_id = parts[1]

        link_value = node.get("link")
        if isinstance(link_value, str):
            next_container_link = link_value

        comment = node.get("comment")
        if isinstance(comment, dict) and not node.get("discardedAt"):
            sharing = node.get("sharing") or {}
            mentions = node.get("mentions") or []
            comments.append(
                {
                    "accountDb": logical_name,
                    "sourceMode": source,
                    "sourceFile": str(source_file),
                    "threadId": thread_id,
                    "threadSubject": thread_subject,
                    "teamId": next_team_id,
                    "containerId": str(node.get("id") or ""),
                    "containerPath": next_container_path,
                    "containerLink": next_container_link,
                    "commentId": str(comment.get("id") or ""),
                    "messageKey": str(node.get("id") or ""),
                    "contentType": str(comment.get("contentType") or ""),
                    "bodyHtml": str(comment.get("body") or ""),
                    "bodyText": html_to_text(str(comment.get("body") or "")),
                    "createdAt": str(comment.get("createdAt") or ""),
                    "clientCreatedAt": str(comment.get("clientCreatedAt") or ""),
                    "sharedAt": str(sharing.get("sharedAt") or ""),
                    "sharedBy": str(sharing.get("by") or ""),
                    "sharedByName": str(sharing.get("name") or ""),
                    "accessRole": str(sharing.get("accessRole") or ""),
                    "mentions": json.dumps(mentions, ensure_ascii=True),
                    "raw": json.dumps(node, ensure_ascii=True),
                }
            )

        for value in node.values():
            walk(
                value,
                team_id=next_team_id,
                container_path=next_container_path,
                container_link=next_container_link,
            )

    walk(payload)
    return comments


def dedupe_comments(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    unique: list[dict[str, str]] = []
    for row in sorted(
        rows,
        key=lambda item: (
            item.get("createdAt", ""),
            item.get("commentId", ""),
            item.get("threadId", ""),
        ),
    ):
        key = "|".join(
            [
                row.get("threadId", ""),
                row.get("commentId", ""),
                row.get("createdAt", ""),
                row.get("bodyText", ""),
            ]
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def write_comments_csv(path: Path, rows: list[dict[str, str]]) -> None:
    import csv

    fieldnames = [
        "accountDb",
        "sourceMode",
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


def extract_live_thread_descriptors(response: dict[str, Any]) -> list[dict[str, str]]:
    descriptors: list[dict[str, str]] = []
    thread_list = response.get("threadList") or []
    if not isinstance(thread_list, list):
        return descriptors

    for item in thread_list:
        if not isinstance(item, dict):
            continue
        thread = item.get("thread") or {}
        if not isinstance(thread, dict):
            continue
        thread_id = thread.get("threadId") or thread.get("id") or ""
        if not isinstance(thread_id, str) or not thread_id:
            continue
        subject = thread.get("subject")
        if not isinstance(subject, str):
            subject = ""
            messages = thread.get("messages") or []
            if isinstance(messages, list):
                for message in messages:
                    if isinstance(message, dict) and isinstance(message.get("subject"), str):
                        subject = str(message["subject"])
                        break
        descriptors.append({"threadId": thread_id, "threadSubject": subject})
    return descriptors


def paginate_live_threads(client: SuperhumanClient, *, page_size: int) -> tuple[list[dict[str, str]], dict[str, Any]]:
    offset = 0
    pages = 0
    seen_offsets: set[int] = set()
    descriptors_by_id: dict[str, dict[str, str]] = {}
    fetched_offsets: list[int] = []

    while True:
        if offset in seen_offsets:
            break
        seen_offsets.add(offset)
        fetched_offsets.append(offset)
        response = client.get_threads(limit=page_size, offset=offset)
        pages += 1

        for descriptor in extract_live_thread_descriptors(response):
            descriptors_by_id.setdefault(descriptor["threadId"], descriptor)

        next_offset = response.get("nextOffset")
        thread_list = response.get("threadList") or []
        if next_offset is None:
            break
        if not isinstance(next_offset, int):
            break
        if not isinstance(thread_list, list) or not thread_list:
            break
        offset = next_offset

    manifest = {
        "pagesFetched": pages,
        "pageSize": page_size,
        "offsets": fetched_offsets,
        "liveThreadCount": len(descriptors_by_id),
    }
    return list(descriptors_by_id.values()), manifest


def paginate_gmail_threads(
    *,
    gmail_access_token: str,
    page_size: int,
    include_spam_trash: bool = True,
    max_threads: int | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    next_page_token: str | None = None
    pages = 0
    descriptors: list[dict[str, str]] = []
    page_tokens: list[str] = []

    while True:
        query = {
            "maxResults": str(page_size),
            "includeSpamTrash": "true" if include_spam_trash else "false",
        }
        if next_page_token:
            query["pageToken"] = next_page_token
        url = "https://content.googleapis.com/gmail/v1/users/me/threads?" + urllib.parse.urlencode(query)
        request = urllib.request.Request(
            url=url,
            headers={
                "Authorization": f"Bearer {gmail_access_token}",
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
                "Origin": "https://mail.superhuman.com",
                "Referer": "https://mail.superhuman.com/",
            },
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=15.0) as response:
            payload = json.loads(response.read().decode("utf-8"))

        pages += 1
        page_tokens.append(next_page_token or "")
        for thread in payload.get("threads") or []:
            thread_id = thread.get("id")
            if isinstance(thread_id, str) and thread_id:
                descriptors.append({"threadId": thread_id, "threadSubject": ""})
                if max_threads is not None and len(descriptors) >= max_threads:
                    return (
                        descriptors,
                        {
                            "pagesFetched": pages,
                            "pageSize": page_size,
                            "pageTokens": page_tokens,
                            "gmailThreadCount": len(descriptors),
                            "resultSizeEstimate": payload.get("resultSizeEstimate"),
                            "truncatedByMaxThreads": True,
                        },
                    )

        next_page_token = payload.get("nextPageToken")
        if not isinstance(next_page_token, str) or not next_page_token:
            break

    return (
        descriptors,
        {
            "pagesFetched": pages,
            "pageSize": page_size,
            "pageTokens": page_tokens,
            "gmailThreadCount": len(descriptors),
            "resultSizeEstimate": payload.get("resultSizeEstimate"),
            "truncatedByMaxThreads": False,
        },
    )


def resolve_cookies(args: argparse.Namespace) -> tuple[str, list[CookieRecord]]:
    if args.cdp_url:
        return load_cdp_superhuman_cookies(args.cdp_url)

    if args.cookie_header:
        return ("manual-cookie-header", parse_cookie_header(args.cookie_header))

    if args.cookie:
        return ("manual-cookie-pairs", parse_cookie_pairs(args.cookie))

    cookie_db, cookies = load_superhuman_cookies(Path(args.profile_dir).expanduser())
    return (str(cookie_db), cookies)


def resolve_gmail_access_token(args: argparse.Namespace) -> str | None:
    if args.gmail_access_token:
        return args.gmail_access_token
    if args.cdp_url:
        try:
            return load_cdp_gmail_access_token(args.cdp_url)
        except Exception:
            return None
    return None


def decode_jwt_claims(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    payload = parts[1]
    payload += "=" * (-len(payload) % 4)
    try:
        raw = base64.urlsafe_b64decode(payload.encode("ascii"))
        decoded = json.loads(raw)
    except (ValueError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def load_cdp_live_context(cdp_url: str) -> CDPLiveContext:
    page = get_cdp_mail_page(cdp_url)
    eval_result = cdp_call(
        str(page["webSocketDebuggerUrl"]),
        "Runtime.evaluate",
        {
            "expression": """
                (() => {
                  const logins = JSON.parse(localStorage.getItem('logins') || '[]');
                  return {
                    href: location.href,
                    title: document.title,
                    logins,
                    scopes: localStorage.getItem((logins[0]?.emailAddress || '') + ':scopes') || null,
                  };
                })()
            """,
            "returnByValue": True,
        },
    )
    value = ((eval_result.get("result") or {}).get("value") or {})
    logins = value.get("logins") or []
    if not isinstance(logins, list) or not logins:
        raise RuntimeError("Could not read logged-in Superhuman account info from the live tab.")
    login = logins[0]
    email = str(login.get("emailAddress") or "")
    team_id = str(login.get("teamId") or login.get("pseudoTeamId") or "")
    if not email:
        raise RuntimeError("Missing emailAddress in Superhuman logins localStorage.")

    events = cdp_network_capture(cdp_url, reload_page=True, duration_seconds=8.0)
    superhuman_headers: dict[str, str] = {}
    gmail_access_token = ""
    for event in events:
        method = event.get("method")
        params = event.get("params") or {}
        if method == "Network.requestWillBeSent":
            request = params.get("request") or {}
            url = str(request.get("url") or "")
            headers = request.get("headers") or {}
            if "/~backend/v3/" in url and not superhuman_headers:
                superhuman_headers.update({str(k): str(v) for k, v in headers.items()})
            if "content.googleapis.com/gmail/v1/users/me/threads" in url:
                authorization = headers.get("Authorization")
                if isinstance(authorization, str) and authorization.startswith("Bearer "):
                    gmail_access_token = authorization.removeprefix("Bearer ").strip()
        elif method == "Network.requestWillBeSentExtraInfo":
            headers = params.get("headers") or {}
            authorization = headers.get("authorization")
            if not gmail_access_token and isinstance(authorization, str) and authorization.startswith("Bearer ya29."):
                gmail_access_token = authorization.removeprefix("Bearer ").strip()

    if not gmail_access_token:
        raise RuntimeError("Could not capture a Gmail API access token from the live browser.")

    auth_header = superhuman_headers.get("Authorization")
    superhuman_id_token = ""
    google_id = ""
    if isinstance(auth_header, str) and auth_header.startswith("Bearer "):
        superhuman_id_token = auth_header.removeprefix("Bearer ").strip()
        claims = decode_jwt_claims(superhuman_id_token)
        google_id = str(claims.get("sub") or "")

    if not google_id:
        userinfo = fetch_google_userinfo(gmail_access_token)
        google_id = str(userinfo.get("sub") or "")
        if not email:
            email = str(userinfo.get("email") or "")
    if not google_id:
        raise RuntimeError("Could not derive Google user id from CDP live session.")

    return CDPLiveContext(
        email=email,
        google_id=google_id,
        team_id=team_id,
        superhuman_id_token=superhuman_id_token,
        gmail_access_token=gmail_access_token,
        session_id=str(superhuman_headers.get("x-superhuman-session-id") or ""),
        device_id=str(superhuman_headers.get("x-superhuman-device-id") or ""),
        version=str(superhuman_headers.get("x-superhuman-version") or SUPERHUMAN_VERSION),
        user_external_id=str(superhuman_headers.get("x-superhuman-user-external-id") or ""),
    )


def run_export(args: argparse.Namespace) -> dict[str, Any]:
    data_dir = Path(args.data_dir).expanduser()
    output_dir = Path(args.output_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    live_context: CDPLiveContext | None = None

    if args.live_only:
        if not args.cdp_url:
            raise RuntimeError("--live-only currently requires --cdp-url so the script can derive live auth context.")
        live_context = load_cdp_live_context(args.cdp_url)
        account = AccountContext(
            logical_name=args.account_email or live_context.email,
            email=args.account_email or live_context.email,
            google_id=args.google_id or live_context.google_id,
            team_id=live_context.team_id,
            team_name="",
            members_count=0,
            source_file=None,
        )
        commented_threads: list[dict[str, str]] = []
    else:
        account, commented_threads = choose_account_context(data_dir, args.account_email)
        if args.cdp_url:
            with contextlib.suppress(Exception):
                live_context = load_cdp_live_context(args.cdp_url)

    google_id = args.google_id or account.google_id

    mode = "cache"
    cookie_source = None
    client: SuperhumanClient | None = None
    gmail_access_token: str | None = None
    errors: list[dict[str, Any]] = []
    crawl_manifest: dict[str, Any] = {
        "strategy": "cache-commented-threads",
        "pagesFetched": 0,
        "pageSize": 0,
        "offsets": [],
        "liveThreadCount": 0,
        "targetThreadCount": len(commented_threads),
    }

    if not args.cache_only:
        try:
            if live_context is not None:
                gmail_access_token = args.gmail_access_token or live_context.gmail_access_token
                if args.id_token or live_context.superhuman_id_token:
                    cookie_source = f"cdp:{args.cdp_url}"
                    client = SuperhumanClient(
                        email=account.email,
                        provider_id=google_id,
                        cookies=[],
                        timeout=args.timeout,
                        id_token=args.id_token or live_context.superhuman_id_token,
                        session_id=live_context.session_id or None,
                        device_id=live_context.device_id or None,
                        version=live_context.version or None,
                        user_external_id=live_context.user_external_id or None,
                    )
                    mode = "live-token"
                else:
                    cookie_source, cookies = resolve_cookies(args)
                    client = SuperhumanClient(
                        email=account.email,
                        provider_id=google_id,
                        cookies=cookies,
                        timeout=args.timeout,
                        id_token=args.id_token,
                        session_id=live_context.session_id or None,
                        device_id=live_context.device_id or None,
                        version=live_context.version or None,
                        user_external_id=live_context.user_external_id or None,
                    )
                    csrf_response = client.get_csrf_token()
                    client.get_tokens(str(csrf_response["csrfToken"]))
                    mode = "live-cookie"
            else:
                cookie_source, cookies = resolve_cookies(args)
                client = SuperhumanClient(
                    email=account.email,
                    provider_id=google_id,
                    cookies=cookies,
                    timeout=args.timeout,
                    id_token=args.id_token,
                )
                if client.id_token:
                    mode = "live-token"
                else:
                    csrf_response = client.get_csrf_token()
                    client.get_tokens(str(csrf_response["csrfToken"]))
                    mode = "live-cookie"
                gmail_access_token = resolve_gmail_access_token(args)
        except Exception as exc:
            errors.append({"stage": "auth", "error": json_safe_error(exc)})
            client = None
            mode = "cache"

    thread_descriptors: list[dict[str, str]]
    if client is not None and gmail_access_token:
        try:
            live_threads, crawl_manifest = paginate_gmail_threads(
                gmail_access_token=gmail_access_token,
                page_size=args.gmail_page_size,
                max_threads=args.max_live_threads,
            )
            crawl_manifest["strategy"] = "live-gmail-threads-pagination-plus-cache-commented-union"
            descriptors_by_id = {
                descriptor["threadId"]: descriptor
                for descriptor in live_threads
            }
            for thread in commented_threads:
                descriptors_by_id.setdefault(
                    thread["threadId"],
                    {
                        "threadId": thread["threadId"],
                        "threadSubject": extract_thread_subject(thread["threadJson"]),
                    },
                )
            thread_descriptors = list(descriptors_by_id.values())
            crawl_manifest["targetThreadCount"] = len(thread_descriptors)
        except Exception as exc:
            errors.append({"stage": "pagination", "error": json_safe_error(exc)})
            if args.live_only:
                raise
            thread_descriptors = [
                {
                    "threadId": thread["threadId"],
                    "threadSubject": extract_thread_subject(thread["threadJson"]),
                }
                for thread in commented_threads
            ]
    elif client is not None:
        errors.append(
            {
                "stage": "pagination",
                "error": {
                    "type": "gmail_access_token_missing",
                    "message": "Could not capture or resolve a Gmail API access token; falling back to cache-known commented threads.",
                },
            }
        )
        if args.live_only:
            raise RuntimeError("Live-only mode could not obtain a Gmail access token for thread enumeration.")
        thread_descriptors = [
            {
                "threadId": thread["threadId"],
                "threadSubject": extract_thread_subject(thread["threadJson"]),
            }
            for thread in commented_threads
        ]
    else:
        if args.live_only:
            raise RuntimeError("Live-only mode could not initialize the live Superhuman client.")
        thread_descriptors = [
            {
                "threadId": thread["threadId"],
                "threadSubject": extract_thread_subject(thread["threadJson"]),
            }
            for thread in commented_threads
        ]

    commented_threads_by_id = {thread["threadId"]: thread for thread in commented_threads}
    all_comments: list[dict[str, str]] = []
    for index, descriptor in enumerate(thread_descriptors, start=1):
        thread_id = descriptor["threadId"]
        thread_subject = descriptor["threadSubject"]
        payload: Any = None
        source_mode = "cache"

        if client is not None:
            try:
                response = client.read_user_data(f"threads/{thread_id}")
                payload = unwrap_userdata_value(response)
                source_mode = mode
            except Exception as exc:
                errors.append(
                    {
                        "stage": "thread",
                        "threadId": thread_id,
                        "error": json_safe_error(exc),
                    }
                )

        if payload is None:
            thread = commented_threads_by_id.get(thread_id)
            if thread is not None:
                try:
                    payload = json.loads((thread or {}).get("superhumanData", "") or "{}")
                except json.JSONDecodeError:
                    payload = {}
            else:
                payload = {}

        all_comments.extend(
            extract_comments_from_payload(
                payload=payload,
                logical_name=account.logical_name,
                source_file=account.source_file,
                thread_id=thread_id,
                thread_subject=thread_subject,
                source=source_mode,
            )
        )

        if index % 25 == 0 or index == len(thread_descriptors):
            print(f"Processed {index}/{len(thread_descriptors)} crawled threads", flush=True)

    unique_comments = dedupe_comments(all_comments)

    json_path = output_dir / f"{args.prefix}.json"
    csv_path = output_dir / f"{args.prefix}.csv"

    payload = {
        "exportedAt": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "cookieSource": cookie_source,
        "crawlManifest": crawl_manifest,
        "account": {
            "logicalName": account.logical_name,
            "email": account.email,
            "googleId": google_id,
            "teamId": account.team_id,
            "teamName": account.team_name,
            "membersCount": account.members_count,
            "sourceFile": str(account.source_file),
            "cacheCommentedThreads": len(commented_threads),
            "crawledThreads": len(thread_descriptors),
        },
        "errors": errors,
        "comments": unique_comments,
    }

    write_json(json_path, payload)
    write_comments_csv(csv_path, unique_comments)

    return {
        "ok": True,
        "mode": mode,
        "account": account.email,
        "teamId": account.team_id,
        "commentedThreads": len(commented_threads),
        "crawledThreads": len(thread_descriptors),
        "commentCount": len(unique_comments),
        "jsonPath": str(json_path),
        "csvPath": str(csv_path),
        "errors": len(errors),
    }


def main() -> int:
    args = parse_args()
    try:
        summary = run_export(args)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": json_safe_error(exc)}), file=sys.stderr)
        return 1

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
