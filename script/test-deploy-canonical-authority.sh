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
  chmod +x "$base/bin"/*
}

run() {
  local base="$1"
  shift
  env -i PATH="$base/bin:$PATH" ROOT="$base/root" SOURCE_BIN="$base/source-bin" SOURCE_APP="$base/app" LIVE_BIN="$base/live-bin" LIVE_APP="$base/live.app" DB="$base/db" SERVE_HUB="$base/hub" BACKUPS_DIR="$base/backups" RUNS_DIR="$base/runs" LOCK_DIR="$base/lock" PASS=test DRAIN_ATTEMPTS="${DRAIN_ATTEMPTS:-900}" SLEEP_SECONDS="${SLEEP_SECONDS:-2}" EXPECTED_HEAD=test-head EXPECTED_BIN_SHA="$(shasum -a 256 "$base/source-bin" | cut -d ' ' -f 1)" EXPECTED_SIDECAR_SHA="$(shasum -a 256 "$base/app/Contents/Resources/sidecar/opencode-gui-coordinator" | cut -d ' ' -f 1)" LIFECYCLE_LOG="$base/lifecycle" /usr/bin/env bash "$SCRIPT" "$@"
}
assert() { "$@" || { printf 'assertion failed: %s\n' "$*" >&2; exit 1; }; }

base="$TMP/dry"; make_fixture "$base"
run "$base" --dry-run
assert test "$(grep -c '/global/drain' "$base/lifecycle" || true)" = 0
assert test ! -e "$base/lock"
assert test ! -e "$base/runs"/*

base="$TMP/singleton"; make_fixture "$base"
mkdir "$base/lock"
set +e
run "$base"
status=$?
set -e
assert test "$status" = 75
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
if DRAIN_ATTEMPTS=1 SLEEP_SECONDS=0 run "$base"; then exit 1; fi
assert test "$(grep -c '/global/drain/cancel' "$base/lifecycle")" = 1
assert test "$(grep -c '/global/drain/begin' "$base/lifecycle")" = 1

base="$TMP/log"; make_fixture "$base"
DEPLOY_CANONICAL_LIBRARY=1 RUNS_DIR="$base/runs" BACKUPS_DIR="$base/backups" LOCK_DIR="$base/lock" RUN_ID=fixed bash -c 'source "$1"; initialize_run; log first; log second; release_lock' bash "$SCRIPT"
assert test "$(grep -c '^.* \(first\|second\)$' "$base/runs/fixed/deploy.log")" = 2

base="$TMP/retention"; make_fixture "$base"
for kind in known-good failed incomplete unknown; do for n in 1 2 3 4; do mkdir "$base/runs/$kind-$n"; printf 'state=%s\n' "$kind" > "$base/runs/$kind-$n/manifest"; sleep 1; done; done
run "$base" --prune-retention
assert test ! -d "$base/runs/known-good-1"
assert test ! -d "$base/runs/failed-1"
assert test -d "$base/runs/incomplete-1"
assert test -d "$base/runs/unknown-1"
printf 'deploy-canonical-authority tests passed\n'
