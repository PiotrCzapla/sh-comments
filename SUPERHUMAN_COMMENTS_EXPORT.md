# Superhuman Comment Export

Run:

```bash
bash "/Users/pczapla/Work/Virgil/superhuman/export_superhuman_comments.sh"
```

What the default exporter does:

1. Reads Chrome's extension-backed Superhuman SQLite files from your local profile.
2. Strips the 4096-byte wrapper Superhuman stores ahead of the SQLite payload.
3. Extracts shared-thread comments from `threads.superhuman_data`.
4. Writes both `superhuman-comments-sqlite-*.json` and `superhuman-comments-sqlite-*.csv`.

Why this is the preferred path:

1. It does not need Apple Events.
2. It does not depend on the active Chrome tab.
3. It reads the extension-owned SQLite storage where Superhuman currently persists shared-thread comments.

Notes:

1. If Chrome is actively mutating the database while you export, rerun after closing the Superhuman tab or Chrome for the cleanest snapshot.
2. The exporter prints a JSON summary with the output paths and comment count.

Legacy fallback:

```bash
bash "/Users/pczapla/Work/Virgil/superhuman/export_superhuman_comments.sh" --page-indexeddb
```

Use this only if you specifically want to compare against the page-visible IndexedDB state.

Requirements for the legacy fallback:

1. Google Chrome must already be signed into your `pc@answer.ai` Superhuman account.
2. The front tab must be `https://mail.superhuman.com/`.
3. If Chrome blocks automation, enable `View > Developer > Allow JavaScript from Apple Events` once, then rerun.
