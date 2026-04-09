#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from shutil import copy2
from typing import Any

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from export_superhuman_comments_sqlite import copy_wrapped_sqlite, export_database, find_databases

COOKIE_SALT = b"saltysalt"
COOKIE_IV = b" " * 16
COOKIE_PBKDF2_ITERATIONS = 1003
CHROME_SAFE_STORAGE_SERVICE = "Chrome Safe Storage"
SUPERHUMAN_VERSION = "2026-03-31T19:06:25Z"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class CookieRecord:
    host_key: str
    name: str
    value: str
    path: str
    is_secure: bool


@dataclass(frozen=True)
class AccountContext:
    logical_name: str
    email: str
    google_id: str
    team_id: str
    team_name: str
    members_count: int
    source_file: Path
    sample_thread_id: str | None
    sample_container_path: str | None
    sample_container_link: str | None
    sample_comment_count: int


class ApiError(RuntimeError):
    def __init__(self, *, method: str, url: str, status: int, body: str) -> None:
        super().__init__(f"{method} {url} -> {status}")
        self.method = method
        self.url = url
        self.status = status
        self.body = body


def default_profile_dir() -> Path:
    return Path.home() / "Library/Application Support/Google/Chrome/Default"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read Superhuman cookies from a Chrome profile, mint a live ID token, "
            "and test live Superhuman endpoints with a readable JSON report."
        )
    )
    parser.add_argument(
        "--profile-dir",
        default=str(default_profile_dir()),
        help="Chrome profile directory. Defaults to ~/Library/Application Support/Google/Chrome/Default.",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory where the JSON report should be written. Defaults to the current directory.",
    )
    parser.add_argument(
        "--account-email",
        help="Account email to probe, for example pc@answer.ai. Defaults to the first non-demo Superhuman account.",
    )
    parser.add_argument(
        "--threads-limit",
        type=int,
        default=10,
        help="How many threads to request from userdata.getThreads. Defaults to 10.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout in seconds. Defaults to 20.",
    )
    return parser.parse_args()


def copy_sqlite_with_sidecars(source_db: Path) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="superhuman-cookie-db-"))
    temp_db = temp_dir / source_db.name
    copy2(source_db, temp_db)

    for suffix in ("-wal", "-shm"):
        sidecar = source_db.with_name(source_db.name + suffix)
        if sidecar.exists():
            copy2(sidecar, temp_dir / sidecar.name)

    return temp_db


def locate_cookie_db(profile_dir: Path) -> Path:
    candidates = [
        profile_dir / "Network" / "Cookies",
        profile_dir / "Cookies",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Could not find a Chrome Cookies database under {profile_dir}")


def get_chrome_safe_storage_password() -> str:
    commands = [
        [
            "security",
            "find-generic-password",
            "-w",
            "-a",
            "Chrome",
            "-s",
            CHROME_SAFE_STORAGE_SERVICE,
        ],
        [
            "security",
            "find-generic-password",
            "-w",
            "-s",
            CHROME_SAFE_STORAGE_SERVICE,
        ],
    ]

    last_error: subprocess.CalledProcessError | None = None
    for command in commands:
        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as exc:
            last_error = exc

    raise RuntimeError(
        "Could not read the Chrome Safe Storage password from macOS Keychain."
    ) from last_error


def derive_cookie_key(password: str) -> bytes:
    return hashlib.pbkdf2_hmac(
        "sha1",
        password.encode("utf-8"),
        COOKIE_SALT,
        COOKIE_PBKDF2_ITERATIONS,
        dklen=16,
    )


def decrypt_cookie_value(encrypted_value: bytes, key: bytes) -> str:
    if not encrypted_value:
        return ""

    if encrypted_value.startswith((b"v10", b"v11")):
        ciphertext = encrypted_value[3:]
        decryptor = Cipher(algorithms.AES(key), modes.CBC(COOKIE_IV)).decryptor()
        padded = decryptor.update(ciphertext) + decryptor.finalize()
        pad_len = padded[-1]
        if pad_len < 1 or pad_len > 16:
            raise ValueError("Invalid Chrome cookie padding")
        return padded[:-pad_len].decode("utf-8", "replace")

    if encrypted_value.startswith(b"v20"):
        raise ValueError("Chrome cookie format v20 is not supported by this script")

    return encrypted_value.decode("utf-8", "replace")


def load_superhuman_cookies(profile_dir: Path) -> tuple[Path, list[CookieRecord]]:
    cookie_db = locate_cookie_db(profile_dir)
    temp_cookie_db = copy_sqlite_with_sidecars(cookie_db)
    key = derive_cookie_key(get_chrome_safe_storage_password())

    connection = sqlite3.connect(str(temp_cookie_db))
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT host_key, name, value, encrypted_value, path, is_secure
            FROM cookies
            WHERE host_key LIKE '%superhuman.com'
            ORDER BY host_key, name
            """
        ).fetchall()
    finally:
        connection.close()

    cookies: list[CookieRecord] = []
    for row in rows:
        value = row["value"] or decrypt_cookie_value(row["encrypted_value"] or b"", key)
        cookies.append(
            CookieRecord(
                host_key=row["host_key"],
                name=row["name"],
                value=value,
                path=row["path"],
                is_secure=bool(row["is_secure"]),
            )
        )

    if not cookies:
        raise RuntimeError("No Superhuman cookies were found in the Chrome profile.")

    return cookie_db, cookies


def host_matches(cookie_host: str, request_host: str) -> bool:
    if cookie_host.startswith("."):
        bare = cookie_host[1:]
        return request_host == bare or request_host.endswith(f".{bare}")
    return request_host == cookie_host


def build_cookie_header(cookies: list[CookieRecord], request_host: str) -> str:
    values: dict[str, str] = {}
    for cookie in cookies:
        if host_matches(cookie.host_key, request_host):
            values[cookie.name] = cookie.value
    return "; ".join(f"{name}={value}" for name, value in values.items())


def cookie_inventory(cookies: list[CookieRecord]) -> dict[str, list[str]]:
    inventory: dict[str, list[str]] = {}
    for cookie in cookies:
        inventory.setdefault(cookie.host_key, []).append(cookie.name)
    return {host: sorted(set(names)) for host, names in sorted(inventory.items())}


def read_general_json(candidate, key: str) -> dict[str, Any]:
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


def choose_account_context(profile_dir: Path, account_email: str | None) -> AccountContext:
    candidates = [
        candidate
        for candidate in find_databases(profile_dir)
        if candidate.logical_name != "demo@superhuman.com.sqlite3"
    ]
    if not candidates:
        raise RuntimeError("No non-demo Superhuman account databases were found.")

    if account_email:
        matching = [
            candidate
            for candidate in candidates
            if candidate.logical_name == f"{account_email}.sqlite3"
            or candidate.logical_name == account_email
        ]
        if not matching:
            available = [candidate.logical_name for candidate in candidates]
            raise RuntimeError(
                f"Account {account_email!r} was not found. Available accounts: {available}"
            )
        candidates = matching

    candidate = candidates[0]
    settings = read_general_json(candidate, "settings")
    comments = export_database(candidate)

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

    grouped_comments: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for comment in comments:
        key = (
            comment.get("threadId", ""),
            comment.get("containerPath", ""),
        )
        grouped_comments.setdefault(key, []).append(comment)

    sample_group: list[dict[str, Any]] = []
    if grouped_comments:
        _, sample_group = max(grouped_comments.items(), key=lambda item: len(item[1]))

    sample_thread_id = sample_group[0].get("threadId") if sample_group else None
    sample_container_path = sample_group[0].get("containerPath") if sample_group else None
    sample_container_link = sample_group[0].get("containerLink") if sample_group else None
    sample_comment_count = len(sample_group)

    team_info = settings.get("teams") or {}
    members = team_info.get("members") or []

    return AccountContext(
        logical_name=candidate.logical_name,
        email=str(preferred_email),
        google_id=str(google_id),
        team_id=str(team_info.get("teamId") or ""),
        team_name=str(team_info.get("name") or ""),
        members_count=len(members) if isinstance(members, list) else 0,
        source_file=candidate.source_file,
        sample_thread_id=sample_thread_id,
        sample_container_path=sample_container_path,
        sample_container_link=sample_container_link,
        sample_comment_count=sample_comment_count,
    )


def decode_jwt_claims(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        return {}

    payload = parts[1]
    payload += "=" * (-len(payload) % 4)
    try:
        raw = base64.urlsafe_b64decode(payload.encode("ascii"))
        claims = json.loads(raw)
    except (ValueError, json.JSONDecodeError):
        return {}

    if not isinstance(claims, dict):
        return {}
    return claims


def token_fingerprint(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


def unwrap_userdata_value(response: dict[str, Any]) -> Any:
    results = response.get("results") or []
    if isinstance(results, list) and results:
        first = results[0]
        if isinstance(first, dict):
            return first.get("value")
    return None


def count_comment_nodes(node: Any) -> int:
    if isinstance(node, dict):
        total = 0
        if isinstance(node.get("comment"), dict) and not node.get("discardedAt"):
            total += 1
        for value in node.values():
            total += count_comment_nodes(value)
        return total

    if isinstance(node, list):
        return sum(count_comment_nodes(item) for item in node)

    return 0


def collect_container_paths(node: Any, paths: set[str]) -> None:
    if isinstance(node, dict):
        path = node.get("path")
        if isinstance(path, str) and path.startswith("teams/"):
            paths.add(path)
        for value in node.values():
            collect_container_paths(value, paths)
        return

    if isinstance(node, list):
        for item in node:
            collect_container_paths(item, paths)


def summarize_threadish_payload(payload: Any) -> dict[str, Any]:
    paths: set[str] = set()
    collect_container_paths(payload, paths)
    return {
        "commentCount": count_comment_nodes(payload),
        "containerPaths": sorted(paths),
    }


def json_safe_error(error: Exception) -> dict[str, Any]:
    if isinstance(error, ApiError):
        return {
            "type": "api_error",
            "status": error.status,
            "method": error.method,
            "url": error.url,
            "body": error.body,
        }
    return {
        "type": error.__class__.__name__,
        "message": str(error),
    }


class SuperhumanClient:
    def __init__(
        self,
        *,
        email: str,
        provider_id: str,
        cookies: list[CookieRecord],
        timeout: float,
    ) -> None:
        self.email = email
        self.provider_id = provider_id
        self.cookies = cookies
        self.timeout = timeout
        self.session_id = str(uuid.uuid4())
        self.device_id = str(uuid.uuid4())
        self.id_token: str | None = None

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
            "x-superhuman-version": SUPERHUMAN_VERSION,
        }

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

    def get_team_members(self, *, allow_bundle_customer: bool = True) -> dict[str, Any]:
        query_path = "/~backend/v3/teams.members"
        if allow_bundle_customer:
            query_path += "?allowBundleCustomer=true"
        return self._request_json(
            base_url="https://mail.superhuman.com",
            path=query_path,
            endpoint="teams.members",
            require_auth=True,
        )

    def read_user_data(
        self,
        relative_path: str | None,
        *,
        page_size: int | None = None,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        full_path = f"users/{self.provider_id}"
        if relative_path:
            full_path = f"{full_path}/{relative_path}"

        body: dict[str, Any] = {
            "reads": [{"path": full_path}],
        }
        if page_size is not None:
            body["pageSize"] = page_size
        if page_token:
            body["pageToken"] = page_token

        return self._request_json(
            base_url="https://mail.superhuman.com",
            path="/~backend/v3/userdata.read",
            endpoint="userdata.read",
            method="POST",
            body=body,
            require_auth=True,
        )

    def get_threads(self, *, limit: int, offset: int = 0) -> dict[str, Any]:
        return self._request_json(
            base_url="https://mail.superhuman.com",
            path="/~backend/v3/userdata.getThreads",
            endpoint="userdata.getThreads",
            method="POST",
            body={
                "offset": offset,
                "limit": limit,
            },
            require_auth=True,
        )

    def open_shared_thread_link(self, path: str) -> dict[str, Any]:
        return self._request_json(
            base_url="https://mail.superhuman.com",
            path="/~backend/v3/links.open",
            endpoint="links.open",
            method="POST",
            body={"path": path},
            require_auth=True,
        )


def run_probe(args: argparse.Namespace) -> dict[str, Any]:
    profile_dir = Path(args.profile_dir).expanduser()
    output_dir = Path(args.output_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)

    cookie_db_path, cookies = load_superhuman_cookies(profile_dir)
    account = choose_account_context(profile_dir, args.account_email)
    client = SuperhumanClient(
        email=account.email,
        provider_id=account.google_id,
        cookies=cookies,
        timeout=args.timeout,
    )

    report: dict[str, Any] = {
        "ranAt": datetime.now(timezone.utc).isoformat(),
        "profileDir": str(profile_dir),
        "cookieDbPath": str(cookie_db_path),
        "cookieInventory": cookie_inventory(cookies),
        "account": {
            "logicalName": account.logical_name,
            "email": account.email,
            "googleId": account.google_id,
            "teamId": account.team_id,
            "teamName": account.team_name,
            "membersCountFromCache": account.members_count,
            "sourceFile": str(account.source_file),
            "sampleThreadId": account.sample_thread_id,
            "sampleContainerPath": account.sample_container_path,
            "sampleContainerLink": account.sample_container_link,
            "sampleCommentCountFromCache": account.sample_comment_count,
        },
        "tests": {},
    }

    csrf_response = client.get_csrf_token()
    report["tests"]["sessions.getCsrfToken"] = {
        "ok": True,
        "response": {
            "expiresIn": csrf_response.get("expiresIn"),
            "hasCsrfToken": bool(csrf_response.get("csrfToken")),
        },
    }

    tokens_response = client.get_tokens(str(csrf_response["csrfToken"]))
    auth_data = tokens_response.get("authData") or {}
    claims = decode_jwt_claims(client.id_token or "")
    report["tests"]["sessions.getTokens"] = {
        "ok": True,
        "response": {
            "aliases": tokens_response.get("aliases") or [],
            "emailAddress": auth_data.get("emailAddress"),
            "scope": auth_data.get("scope"),
            "expiresIn": auth_data.get("expiresIn"),
            "tokenFingerprint": token_fingerprint(client.id_token or ""),
            "claims": {
                key: claims.get(key)
                for key in ("iss", "aud", "sub", "email", "email_verified", "iat", "exp", "hd")
                if key in claims
            },
        },
    }

    try:
        team_members = client.get_team_members(allow_bundle_customer=True)
        report["tests"]["teams.members"] = {
            "ok": True,
            "response": team_members,
            "summary": {
                "memberCount": len(team_members.get("members") or []),
                "inviteCount": len(team_members.get("invites") or []),
                "userEmail": (team_members.get("user") or {}).get("emailAddress"),
            },
        }
    except Exception as exc:
        report["tests"]["teams.members"] = {"ok": False, "error": json_safe_error(exc)}

    try:
        settings_response = client.read_user_data("settings", page_size=100)
        settings_value = unwrap_userdata_value(settings_response)
        report["tests"]["userdata.read settings"] = {
            "ok": True,
            "response": settings_response,
            "summary": {
                "keys": sorted(settings_value.keys()) if isinstance(settings_value, dict) else [],
                "teamId": ((settings_value or {}).get("teams") or {}).get("teamId")
                if isinstance(settings_value, dict)
                else None,
                "currentHistoryId": settings_response.get("currentHistoryId"),
                "pageToken": settings_response.get("pageToken"),
            },
        }
    except Exception as exc:
        report["tests"]["userdata.read settings"] = {"ok": False, "error": json_safe_error(exc)}

    if account.sample_thread_id:
        try:
            thread_response = client.read_user_data(f"threads/{account.sample_thread_id}", page_size=100)
            thread_value = unwrap_userdata_value(thread_response)
            report["tests"]["userdata.read sample_thread"] = {
                "ok": True,
                "response": thread_response,
                "summary": {
                    "threadId": account.sample_thread_id,
                    **summarize_threadish_payload(thread_value),
                },
            }
        except Exception as exc:
            report["tests"]["userdata.read sample_thread"] = {"ok": False, "error": json_safe_error(exc)}

    if account.sample_container_path:
        try:
            link_response = client.open_shared_thread_link(account.sample_container_path)
            report["tests"]["links.open"] = {
                "ok": True,
                "response": link_response,
                "summary": {
                    "path": account.sample_container_path,
                    **summarize_threadish_payload(link_response),
                },
            }
        except Exception as exc:
            report["tests"]["links.open"] = {"ok": False, "error": json_safe_error(exc)}

    try:
        threads_response = client.get_threads(limit=args.threads_limit, offset=0)
        thread_list = threads_response.get("threadList") or []
        thread_ids = []
        if isinstance(thread_list, list):
            for item in thread_list[:20]:
                if isinstance(item, dict):
                    thread = item.get("thread") or {}
                    if isinstance(thread, dict) and thread.get("threadId"):
                        thread_ids.append(thread["threadId"])

        report["tests"]["userdata.getThreads"] = {
            "ok": True,
            "response": threads_response,
            "summary": {
                "threadCount": len(thread_list) if isinstance(thread_list, list) else 0,
                "nextOffset": threads_response.get("nextOffset"),
                "sampleThreadPresent": account.sample_thread_id in thread_ids if account.sample_thread_id else None,
                "threadIds": thread_ids,
            },
        }
    except Exception as exc:
        report["tests"]["userdata.getThreads"] = {"ok": False, "error": json_safe_error(exc)}

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    report_path = output_dir / f"superhuman-live-cookie-probe-{stamp}.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    report["reportPath"] = str(report_path)
    return report


def main() -> int:
    args = parse_args()
    try:
        report = run_probe(args)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": json_safe_error(exc)}), file=sys.stderr)
        return 1

    summary = {
        "ok": True,
        "reportPath": report["reportPath"],
        "account": report["account"]["email"],
        "teamId": report["account"]["teamId"],
        "tests": {
            name: details.get("ok", False)
            for name, details in report["tests"].items()
        },
    }
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
