#!/usr/bin/env bash

set -euo pipefail

OUT_DIR="${1:-$HOME/Downloads}"
mkdir -p "$OUT_DIR"
TMP_JS="$(mktemp "${TMPDIR:-/tmp}/superhuman-comments.XXXXXX.js")"
trap 'rm -f "$TMP_JS"' EXIT

read -r -d '' JS_PAYLOAD <<'EOF' || true
(async () => {
  const now = new Date();
  const stamp = now.toISOString().replace(/[:.]/g, "-");

  const isObject = (value) => value && typeof value === "object";

  const safeJson = (value) => {
    try {
      return JSON.parse(JSON.stringify(value));
    } catch {
      return value;
    }
  };

  const collectDbRecords = async () => {
    if (!indexedDB.databases) return [];
    const databases = await indexedDB.databases();
    const out = [];

    for (const dbMeta of databases) {
      if (!dbMeta?.name) continue;
      await new Promise((resolve) => {
        const openReq = indexedDB.open(dbMeta.name, dbMeta.version);
        openReq.onerror = () => resolve();
        openReq.onblocked = () => resolve();
        openReq.onsuccess = () => {
          const db = openReq.result;
          const storeNames = Array.from(db.objectStoreNames || []);
          if (!storeNames.length) {
            db.close();
            resolve();
            return;
          }

          let pending = storeNames.length;
          const done = () => {
            pending -= 1;
            if (pending === 0) {
              db.close();
              resolve();
            }
          };

          for (const storeName of storeNames) {
            try {
              const tx = db.transaction(storeName, "readonly");
              const store = tx.objectStore(storeName);
              const req = store.getAll();
              req.onerror = done;
              req.onsuccess = () => {
                const values = Array.isArray(req.result) ? req.result : [];
                out.push({
                  database: dbMeta.name,
                  store: storeName,
                  count: values.length,
                  values,
                });
                done();
              };
            } catch {
              done();
            }
          }
        };
      });
    }

    return out;
  };

  const looksLikeComment = (value, path) => {
    if (!isObject(value)) return false;
    const lowerPath = path.join(".").toLowerCase();
    if (lowerPath.includes(".comment")) return true;
    if ("comment" in value && isObject(value.comment)) return true;
    if ("body" in value && ("clientCreatedAt" in value || "createdAt" in value) && ("id" in value || "authorName" in value)) return true;
    return false;
  };

  const flattenComment = (comment, ctx) => {
    const body = comment?.body ?? comment?.comment?.body ?? "";
    const authorName = comment?.authorName ?? comment?.comment?.authorName ?? "";
    const createdAt = comment?.clientCreatedAt ?? comment?.createdAt ?? comment?.comment?.clientCreatedAt ?? comment?.comment?.createdAt ?? "";
    const id = comment?.id ?? comment?.comment?.id ?? "";
    const mentions = comment?.mentions ?? comment?.comment?.mentions ?? [];
    const discardedAt = comment?.discardedAt ?? "";
    return {
      id,
      body,
      authorName,
      createdAt,
      discardedAt,
      mentions: Array.isArray(mentions) ? mentions.map((m) => (typeof m === "string" ? m : m?.email || m?.name || JSON.stringify(m))).join(", ") : "",
      database: ctx.database,
      store: ctx.store,
      sourcePath: ctx.path.join("."),
      threadId: ctx.threadId || "",
      teamId: ctx.teamId || "",
      containerId: ctx.containerId || "",
      raw: safeJson(comment),
    };
  };

  const extracted = [];
  const seen = new Set();

  const visit = (value, ctx) => {
    if (!isObject(value)) return;

    if (looksLikeComment(value, ctx.path)) {
      const normalized = flattenComment(value.comment ?? value, ctx);
      const key = normalized.id || `${normalized.teamId}|${normalized.threadId}|${normalized.createdAt}|${normalized.body}`;
      if (!seen.has(key) && normalized.body) {
        seen.add(key);
        extracted.push(normalized);
      }
    }

    if (ctx.path.length > 18) return;

    if (Array.isArray(value)) {
      value.forEach((item, index) => visit(item, { ...ctx, path: [...ctx.path, String(index)] }));
      return;
    }

    for (const [key, child] of Object.entries(value)) {
      const nextCtx = { ...ctx, path: [...ctx.path, key] };
      if (ctx.path.includes("teams") && !ctx.teamId && key !== "teams" && isObject(child)) nextCtx.teamId = key;
      if (ctx.path.includes("containers") && !ctx.containerId && key !== "containers" && isObject(child)) nextCtx.containerId = key;
      if (key === "threadId" && typeof child === "string") nextCtx.threadId = child;
      visit(child, nextCtx);
    }
  };

  const dbRecords = await collectDbRecords();
  for (const bucket of dbRecords) {
    for (const value of bucket.values) {
      visit(value, {
        database: bucket.database,
        store: bucket.store,
        path: [bucket.database, bucket.store],
        teamId: "",
        threadId: value?.threadId || "",
        containerId: "",
      });
    }
  }

  extracted.sort((a, b) => String(a.createdAt).localeCompare(String(b.createdAt)));

  const summary = {
    exportedAt: now.toISOString(),
    databaseBucketsScanned: dbRecords.length,
    commentsFound: extracted.length,
  };

  const jsonBlob = new Blob([JSON.stringify({ summary, comments: extracted }, null, 2)], { type: "application/json" });

  const csvEscape = (v) => `"${String(v ?? "").replaceAll('"', '""').replace(/\n/g, "\\n")}"`;
  const csvHeaders = ["id", "createdAt", "authorName", "body", "mentions", "discardedAt", "threadId", "teamId", "containerId", "database", "store", "sourcePath"];
  const csvRows = [
    csvHeaders.join(","),
    ...extracted.map((row) => csvHeaders.map((h) => csvEscape(row[h])).join(",")),
  ].join("\n");
  const csvBlob = new Blob([csvRows], { type: "text/csv" });

  const download = (blob, filename) => {
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    setTimeout(() => {
      URL.revokeObjectURL(a.href);
      a.remove();
    }, 2000);
  };

  download(jsonBlob, `superhuman-comments-${stamp}.json`);
  download(csvBlob, `superhuman-comments-${stamp}.csv`);

  return JSON.stringify(summary);
})()
EOF

printf '%s' "$JS_PAYLOAD" > "$TMP_JS"

if ! output=$(osascript - "$TMP_JS" <<'APPLESCRIPT' 2>&1
on run argv
  set jsFile to item 1 of argv
  set jsPayload to do shell script "cat " & quoted form of jsFile
  tell application "Google Chrome"
    if (count of windows) is 0 then error "Google Chrome has no open windows."
    set theTab to active tab of front window
    set currentUrl to URL of theTab
    if currentUrl does not start with "https://mail.superhuman.com/" then
      error "Front tab is not https://mail.superhuman.com/."
    end if
    execute theTab javascript jsPayload
  end tell
end run
APPLESCRIPT
); then
  if [[ "$output" == *"Allow JavaScript from Apple Events"* ]]; then
    cat <<'EOF' >&2
Chrome is blocking scripted page access.

One-time fix in Google Chrome:
1. Focus Chrome.
2. Open the menu `View > Developer`.
3. Enable `Allow JavaScript from Apple Events`.
4. Re-run this script while https://mail.superhuman.com/ is the front tab.
EOF
    exit 2
  fi
  printf '%s\n' "$output" >&2
  exit 1
fi

printf '%s\n' "$output"
