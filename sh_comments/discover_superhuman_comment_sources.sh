#!/usr/bin/env zsh

set -euo pipefail

ALL_BROWSERS=1
PROFILE_FILTER=""

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "sqlite3 is required but was not found on PATH" >&2
  exit 1
fi

if ! sqlite3 ':memory:' "select json_extract('{\"a\":1}', '\$.a');" >/dev/null 2>&1; then
  echo "sqlite3 JSON support is required but unavailable" >&2
  exit 1
fi

parse_args() {
  while (( $# > 0 )); do
    case "$1" in
      --chrome-only)
        ALL_BROWSERS=0
        shift
        ;;
      --profile-dir)
        PROFILE_FILTER=$2
        shift 2
        ;;
      *)
        echo "unknown argument: $1" >&2
        exit 1
        ;;
    esac
  done
}

display_path() {
  local path=$1
  if [[ "$path" == "$HOME"* ]]; then
    printf '~%s' "${path#$HOME}"
  else
    printf '%s' "$path"
  fi
}

display_date() {
  local value=$1
  printf '%s' "${value:0:10}"
}

profile_dirs_under() {
  local root=$1
  local dir
  [[ -d "$root" ]] || return 0

  setopt local_options null_glob
  for dir in \
    "$root/Default" \
    "$root/Guest Profile" \
    "$root/System Profile" \
    "$root"/Profile\ *; do
    [[ -d "$dir" ]] && printf '%s\n' "$dir"
  done
}

discover_profiles() {
  local home_dir=$HOME
  local root

  if [[ -n "$PROFILE_FILTER" ]]; then
    [[ -d "$PROFILE_FILTER" ]] && printf '%s\n' "$PROFILE_FILTER"
    return 0
  fi

  profile_dirs_under "$home_dir/Library/Application Support/Google/Chrome"

  if (( ALL_BROWSERS )); then
    for root in \
      "$home_dir/Library/Application Support/Arc/User Data" \
      "$home_dir/Library/Application Support/BraveSoftware/Brave-Browser" \
      "$home_dir/Library/Application Support/Microsoft Edge" \
      "$home_dir/Library/Application Support/Chromium"; do
      profile_dirs_under "$root"
    done
  fi

  [[ -d "$home_dir/Library/Application Support/Superhuman" ]] && \
    printf '%s\n' "$home_dir/Library/Application Support/Superhuman"
}

find_wrapped_db_files() {
  local profile_dir=$1
  local file_system_dir="$profile_dir/File System"
  local db_file

  [[ -d "$file_system_dir" ]] || return 0

  setopt local_options null_glob
  for db_file in "$file_system_dir"/[0-9][0-9][0-9]/t/00/*(.N); do
    printf '%s\n' "$db_file"
  done
}

is_wrapped_sqlite() {
  local db_file=$1
  local magic
  magic=$(dd if="$db_file" bs=4096 skip=1 count=1 2>/dev/null | LC_ALL=C cut -c1-16 || true)
  [[ "$magic" == "SQLite format 3"* ]]
}

extract_logical_name() {
  local db_file=$1
  dd if="$db_file" bs=4096 count=1 2>/dev/null | perl -ne 'if (/\/([^\/\0]+\.sqlite3)/) { print $1; exit }'
}

query_wrapped_db() {
  local db_file=$1
  local tmp_db

  tmp_db=$(mktemp "${TMPDIR:-/tmp}/superhuman-comments-XXXXXXXX")
  dd if="$db_file" of="$tmp_db" bs=4096 skip=1 status=none 2>/dev/null

  sqlite3 -readonly -batch -separator '|' "$tmp_db" "
    SELECT
      count(*),
      count(DISTINCT thread_id),
      ifnull(min(created_at), ''),
      ifnull(max(created_at), '')
    FROM (
      SELECT
        threads.thread_id AS thread_id,
        ifnull(json_extract(message.value, '\$.comment.createdAt'), '') AS created_at
      FROM threads
      JOIN json_each(threads.superhuman_data, '\$.teams') AS team
      JOIN json_each(team.value, '\$.containers') AS container
      JOIN json_each(container.value, '\$.messages') AS message
      WHERE threads.superhuman_data LIKE '%\"comment\"%'
        AND json_extract(message.value, '\$.comment') IS NOT NULL
    );
  "

  rm -f "$tmp_db"
}

summarize_profile() {
  local profile_dir=$1
  local db_file
  local logical_name
  local row
  local count
  local thread_count
  local first
  local last
  local total_comments=0
  local total_threads=0
  local first_comment=""
  local last_comment=""
  local addresses=""

  while IFS= read -r db_file; do
    [[ -n "$db_file" ]] || continue
    is_wrapped_sqlite "$db_file" || continue

    logical_name=$(extract_logical_name "$db_file")
    if [[ -n "$logical_name" ]]; then
      if [[ -z "$addresses" ]]; then
        addresses=$logical_name
      elif [[ ",$addresses," != *",$logical_name,"* ]]; then
        addresses="${addresses},${logical_name}"
      fi
    fi

    row=$(query_wrapped_db "$db_file")
    count=${row%%|*}
    row=${row#*|}
    thread_count=${row%%|*}
    row=${row#*|}
    first=${row%%|*}
    last=${row#*|}

    total_comments=$((total_comments + count))
    total_threads=$((total_threads + thread_count))
    [[ -n "$first" && ( -z "$first_comment" || "$first" < "$first_comment" ) ]] && first_comment=$first
    [[ -n "$last" && ( -z "$last_comment" || "$last" > "$last_comment" ) ]] && last_comment=$last
  done < <(find_wrapped_db_files "$profile_dir")

  [[ "$total_comments" -gt 0 ]] || return 0
  printf '%-58s  %6s  %6s  %-10s  %-10s  %s\n' \
    "$(display_path "$profile_dir")" \
    "$total_comments" \
    "$total_threads" \
    "$(display_date "$first_comment")" \
    "$(display_date "$last_comment")" \
    "$addresses"
}

main() {
  local profile_dir
  parse_args "$@"

  printf '%-58s  %6s  %6s  %-10s  %-10s  %s\n' \
    "path" \
    "comments" \
    "threads" \
    "first" \
    "last" \
    "addresses"

  while IFS= read -r profile_dir; do
    [[ -n "$profile_dir" ]] || continue
    summarize_profile "$profile_dir"
  done < <(discover_profiles)
}

main "$@"
