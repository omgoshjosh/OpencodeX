#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/deploy-canonical-authority.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

make_fixture() {
  local base="$1"
  mkdir -p "$base/root/.git" "$base/app/Contents/Resources/sidecar" "$base/live.app" "$base/backups" "$base/runs" "$base/bin"
  printf bin > "$base/source-bin"; chmod +x "$base/source-bin"
  printf sidecar > "$base/app/Contents/Resources/sidecar/opencode-gui-coordinator"
  printf live > "$base/live-bin"; chmod +x "$base/live-bin"
  printf db > "$base/db"; printf hub > "$base/hub"
  cat > "$base/bin/git" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$EXPECTED_HEAD"
EOF
  cat > "$base/bin/codesign" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  cat > "$base/bin/curl" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$LIFECYCLE_LOG"
case "$*" in
  *'/global/health'*) printf '{"runID":"run-1","healthy":true,"accepting":true}\n' ;;
  *'/global/drain/cancel'*) printf '{"ok":true}\n' ;;
  *'/global/drain'*) printf '{"ready":false}\n' ;;
esac
EOF
  cat > "$base/bin/df" <<'EOF'
#!/usr/bin/env bash
printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\nmock 1 1 999999999 1%% /\n'
EOF
  cat > "$base/bin/sqlite3" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  cat > "$base/bin/launchctl" <<'EOF'
#!/usr/bin/env bash
case "$1" in
  print) printf 'runs = 1\n' ;;
  kickstart) exit 0 ;;
esac
EOF
  chmod +x "$base/bin"/*
}

run() {
  local base="$1"
  shift
  env -i PATH="$base/bin:$PATH" ROOT="$base/root" SOURCE_BIN="$base/source-bin" SOURCE_APP="$base/app" LIVE_BIN="$base/live-bin" LIVE_APP="$base/live.app" DB="$base/db" SERVE_HUB="$base/hub" BACKUPS_DIR="$base/backups" RUNS_DIR="$base/runs" LOCK_DIR="$base/lock" RUN_ID="${RUN_ID:-test-run}" PASS=test DEPLOYMENT_ID="${DEPLOYMENT_ID:-test-$RANDOM}" DRAIN_TIMEOUT_SECONDS="${DRAIN_TIMEOUT_SECONDS:-1800}" DRAIN_POLL_SECONDS="${DRAIN_POLL_SECONDS:-2}" SLEEP_SECONDS="${SLEEP_SECONDS:-2}" EXPECTED_HEAD=test-head EXPECTED_BIN_SHA="$(shasum -a 256 "$base/source-bin" | cut -d ' ' -f 1)" EXPECTED_SIDECAR_SHA="$(shasum -a 256 "$base/app/Contents/Resources/sidecar/opencode-gui-coordinator" | cut -d ' ' -f 1)" LIFECYCLE_LOG="$base/lifecycle" /usr/bin/env bash "$SCRIPT" "$@"
}
assert() { "$@" || { printf 'assertion failed: %s\n' "$*" >&2; exit 1; }; }

snapshot_under_writes() {
  local dir
  dir="$(mktemp -d)"
  trap "rm -rf '$dir'" RETURN
  sqlite3 "$dir/src.db" \
    "pragma journal_mode=wal; create table t(id integer primary key, b blob);"
  sqlite3 "$dir/src.db" \
    "insert into t(b) select randomblob(4096) from generate_series(1,40000);"

  (
    for _ in $(seq 1 100000); do
      sqlite3 "$dir/src.db" "insert into t(b) values(randomblob(2048));" 2>/dev/null || true
    done
  ) &
  local writer=$!
  sleep 1

  local start deadline=60 rc
  start="$(date +%s)"
  sqlite3 "$dir/src.db" "VACUUM INTO '$dir/out.db'" >/dev/null 2>&1 &
  local snap=$!
  while :; do
    if ! kill -0 "$snap" 2>/dev/null; then
      wait "$snap"
      rc=$?
      break
    fi
    if (( $(date +%s) - start >= deadline )); then
      kill -9 "$snap" 2>/dev/null || true
      rc=124
      break
    fi
    sleep 1
  done
  kill "$writer" 2>/dev/null || true
  wait "$writer" 2>/dev/null || true

  [ "$rc" = 0 ] || { printf 'snapshot did not converge under writes (rc=%s)\n' "$rc"; return 1; }
  [ "$(sqlite3 "$dir/out.db" 'pragma integrity_check;' | head -1)" = ok ] || {
    printf 'snapshot failed integrity_check\n'
    return 1
  }
}

base="$TMP/dry"; make_fixture "$base"
run "$base" --dry-run
assert test "$(grep -c '/global/drain' "$base/lifecycle" || true)" = 0
assert test ! -e "$base/lock"
assert test ! -e "$base/runs"/*
assert test ! -e "$base/runs"/.deployment-token-*

base="$TMP/singleton"; make_fixture "$base"
mkdir "$base/lock"
set +e
run "$base"
status=$?
set -e
assert test "$status" = 75
assert test ! -e "$base/lifecycle"

base="$TMP/live-disk"; make_fixture "$base"
cat > "$base/bin/df" <<'EOF'
#!/usr/bin/env bash
case "$*" in
  *backups*) printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\nbackup 1 1 999999999 1%% /backup\n' ;;
  *) printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\nlive 1 1 1 99%% /live\n' ;;
esac
EOF
chmod +x "$base/bin/df"
if run "$base" --dry-run; then exit 1; fi
assert test ! -e "$base/lifecycle"

base="$TMP/disk"; make_fixture "$base"
cat > "$base/bin/df" <<'EOF'
#!/usr/bin/env bash
printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\nmock 1 1 1 99%% /\n'
EOF
chmod +x "$base/bin/df"
if run "$base" --dry-run; then exit 1; fi
assert test ! -e "$base/lifecycle"

base="$TMP/cancel"; make_fixture "$base"
if DRAIN_TIMEOUT_SECONDS=1 DRAIN_POLL_SECONDS=0.1 run "$base"; then exit 1; fi
assert test "$(grep -c '/global/drain/cancel' "$base/lifecycle")" = 1
assert test "$(grep -c '/global/drain/begin' "$base/lifecycle")" = 1

base="$TMP/token"; make_fixture "$base"
if DEPLOYMENT_ID=keepalive DRAIN_TIMEOUT_SECONDS=1 DRAIN_POLL_SECONDS=0.1 run "$base"; then exit 1; fi
begins="$(grep -c '/global/drain/begin' "$base/lifecycle")"
set +e
DEPLOYMENT_ID=keepalive run "$base"
status=$?
set -e
assert test "$status" = 75
assert test "$(grep -c '/global/drain/begin' "$base/lifecycle")" = "$begins"

base="$TMP/log"; make_fixture "$base"
DEPLOY_CANONICAL_LIBRARY=1 RUNS_DIR="$base/runs" BACKUPS_DIR="$base/backups" LOCK_DIR="$base/lock" RUN_ID=fixed bash -c 'source "$1"; acquire_lock; initialize_run; log first; log second; release_lock' bash "$SCRIPT"
assert test "$(grep -c '^.* \(first\|second\)$' "$base/runs/fixed/deploy.log")" = 2

base="$TMP/retention"; make_fixture "$base"
mkdir "$base/backups/deploy-current"; printf 'state=known-good\n' > "$base/backups/deploy-current/manifest"
for kind in known-good failed incomplete unknown; do for n in 1 2 3 4; do mkdir "$base/backups/deploy-$kind-$n"; printf 'state=%s\n' "$kind" > "$base/backups/deploy-$kind-$n/manifest"; sleep 1; done; done
RUN_ID=current run "$base" --prune-retention
assert test ! -d "$base/backups/deploy-known-good-1"
assert test ! -d "$base/backups/deploy-failed-1"
assert test -d "$base/backups/deploy-incomplete-1"
assert test -d "$base/backups/deploy-unknown-1"
assert test -d "$base/backups/deploy-current"
assert grep -F 'VACUUM INTO' "$SCRIPT"
assert test "$(grep -c '".backup ' "$SCRIPT" || true)" = 0
assert snapshot_under_writes
printf 'deploy-canonical-authority tests passed\n'
