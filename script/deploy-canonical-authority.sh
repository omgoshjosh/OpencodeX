#!/usr/bin/env bash
# Deploy pinned canonical artifacts without leaving a drained service behind.
set -Eeuo pipefail
umask 077

ROOT="${ROOT:-/Users/josh/agents/worktrees/restart-drain-replay}"
SOURCE_BIN="${SOURCE_BIN:-$ROOT/packages/opencode/dist/opencode-darwin-arm64/bin/opencode}"
SOURCE_APP="${SOURCE_APP:-$ROOT/packages/gui/release/mac-arm64/opencodex-gui.app}"
LIVE_BIN="${LIVE_BIN:-$HOME/.opencode/bin/opencodex}"
LIVE_APP="${LIVE_APP:-/Applications/OpencodeX.app}"
DB="${DB:-$HOME/.local/share/opencode/opencode.db}"
SERVE_HUB="${SERVE_HUB:-$HOME/.opencode/serve-hub.sh}"
BACKUPS_DIR="${BACKUPS_DIR:-$HOME/.opencode/backups}"
RUNS_DIR="${RUNS_DIR:-$HOME/.opencode/deploy-runs}"
LOCK_DIR="${LOCK_DIR:-$HOME/.opencode/deploy-canonical-authority.lock}"
EXPECTED_HEAD="${EXPECTED_HEAD:-1110f2a12aa1735f228406fd4a0ac3769df6f00d}"
EXPECTED_BIN_SHA="${EXPECTED_BIN_SHA:-88daba952609ffdf199b4c3fee93cc4fbe716319dafcb4d19072a1d009da0faf}"
EXPECTED_SIDECAR_SHA="${EXPECTED_SIDECAR_SHA:-d978c55967135134d86f76a4468d58a57ff3852731c62e1fc78f5d168d4c7c68}"
SAFETY_KIB="${SAFETY_KIB:-1048576}"
DRAIN_TIMEOUT_SECONDS="${DRAIN_TIMEOUT_SECONDS:-1800}"
DRAIN_POLL_SECONDS="${DRAIN_POLL_SECONDS:-2}"
HEALTH_ATTEMPTS="${HEALTH_ATTEMPTS:-30}"
SLEEP_SECONDS="${SLEEP_SECONDS:-2}"
PASS="${PASS:-$(<"$HOME/.opencode/serve.pass")}"
AUTH="opencode:$PASS"
RUN_ID="${RUN_ID:-$(date +%Y%m%d%H%M%S)-$$}"
DEPLOYMENT_ID="${DEPLOYMENT_ID:-}"
RUN_DIR="$RUNS_DIR/$RUN_ID"
LOG_FILE="$RUN_DIR/deploy.log"
MANIFEST="$RUN_DIR/manifest"
BACKUP="$BACKUPS_DIR/deploy-$RUN_ID"
TOKEN_DIR=""
NEW_APP="${LIVE_APP}.deploy-$RUN_ID"
OLD_APP="${LIVE_APP}.previous-$RUN_ID"
DRY_RUN=0
DRAIN_ACTIVE=0
CUTOVER_STARTED=0
CANCELLED=0
LOCK_HELD=0
ROLLBACK_DONE=0
ORIGINAL_RUN_ID=""
ORIGINAL_RUN_COUNT=""

usage() { printf 'usage: %s [--dry-run|--prune-retention] (DEPLOYMENT_ID is required)\n' "$0" >&2; }
log() { printf '%s %s\n' "$(date '+%F %T')" "$*" >> "$LOG_FILE"; }
state() { printf 'phase=%s\nstate=%s\nat=%s\n' "$1" "$2" "$(date -u +%FT%TZ)" >> "$MANIFEST"; [ ! -d "$BACKUP" ] || printf 'phase=%s\nstate=%s\nat=%s\n' "$1" "$2" "$(date -u +%FT%TZ)" >> "$BACKUP/manifest"; log "phase=$1 state=$2"; }
health() { curl -fsS --max-time 5 -u "$AUTH" "http://127.0.0.1:4096/global/health"; }
restart_backend() { launchctl kickstart -k "gui/$(id -u)/ai.opencode.serve"; }
service_run_count() { launchctl print "gui/$(id -u)/ai.opencode.serve" | awk '$1 == "runs" && $2 == "=" && $3 ~ /^[0-9]+$/ { print $3; exit }'; }
sha256() { shasum -a 256 "$1" | cut -d ' ' -f 1; }
size_kib() { du -sk "$1" | cut -f 1; }
available_kib() { df -Pk "$1" | awk 'NR == 2 { print $4 }'; }

die() { printf '%s\n' "deploy-canonical-authority: $*" >&2; return 1; }

acquire_lock() {
  if ! mkdir "$LOCK_DIR"; then
    printf 'deploy-canonical-authority: another deployment is running\n' >&2
    exit 75
  fi
  LOCK_HELD=1
}

initialize_run() {
  mkdir "$RUN_DIR"
  : >> "$LOG_FILE"
  state initialized running
}

consume_deployment_id() {
  [[ "$DEPLOYMENT_ID" =~ ^[A-Za-z0-9._-]+$ ]] || die "DEPLOYMENT_ID must contain only letters, digits, dot, underscore, or dash"
  TOKEN_DIR="$RUNS_DIR/.deployment-token-$DEPLOYMENT_ID"
  if ! mkdir "$TOKEN_DIR"; then
    printf 'deploy-canonical-authority: deployment ID was already consumed\n' >&2
    release_lock
    exit 75
  fi
  printf 'deployment_id=%s\nrun_id=%s\nconsumed_at=%s\n' "$DEPLOYMENT_ID" "$RUN_ID" "$(date -u +%FT%TZ)" > "$TOKEN_DIR/consumed"
}

release_lock() {
  if (( LOCK_HELD )); then rmdir "$LOCK_DIR"; LOCK_HELD=0; fi
}

cancel_drain_once() {
  (( DRAIN_ACTIVE )) || return 0
  (( CUTOVER_STARTED == 0 )) || return 0
  (( CANCELLED == 0 )) || return 0
  CANCELLED=1
  log "drain=cancel requested_run=$ORIGINAL_RUN_ID"
  curl -fsS --max-time 5 -u "$AUTH" -H 'Content-Type: application/json' \
    -d "{\"expectedRunID\":\"$ORIGINAL_RUN_ID\"}" \
    "http://127.0.0.1:4096/global/drain/cancel"
  local recovered
  recovered="$(health)"
  [ "$(printf '%s' "$recovered" | jq -r '.accepting // false')" = true ] || return 1
  DRAIN_ACTIVE=0
  log 'drain=cancelled accepting=true'
}

on_exit() {
  local status="$?"
  trap - EXIT INT TERM HUP
  if (( DRAIN_ACTIVE && CUTOVER_STARTED == 0 )); then
    if ! cancel_drain_once; then
      log 'drain=cancel-failed'
      status=1
    fi
  fi
  if (( status != 0 && CUTOVER_STARTED && ROLLBACK_DONE == 0 )); then
    if ! rollback; then status=1; fi
  fi
  release_lock
  exit "$status"
}

on_signal() { exit 1; }

preflight() {
  [ -d "$ROOT/.git" ] || die "source repository is unavailable"
  [ "$(git -C "$ROOT" rev-parse HEAD)" = "$EXPECTED_HEAD" ] || die "source head changed"
  [ -x "$SOURCE_BIN" ] || die "source binary is unavailable"
  [ -d "$SOURCE_APP" ] || die "source app is unavailable"
  [ -f "$SOURCE_APP/Contents/Resources/sidecar/opencode-gui-coordinator" ] || die "source sidecar is unavailable"
  [ "$(sha256 "$SOURCE_BIN")" = "$EXPECTED_BIN_SHA" ] || die "backend checksum mismatch"
  [ "$(sha256 "$SOURCE_APP/Contents/Resources/sidecar/opencode-gui-coordinator")" = "$EXPECTED_SIDECAR_SHA" ] || die "sidecar checksum mismatch"
  codesign --verify --deep --strict "$SOURCE_APP" || die "source app signature invalid"
  [ -x "$LIVE_BIN" ] || die "live binary is unavailable"
  [ -d "$LIVE_APP" ] || die "live app is unavailable"
  [ -f "$DB" ] || die "database is unavailable"
  [ -f "$SERVE_HUB" ] || die "serve hub is unavailable"
  [ -d "$BACKUPS_DIR" ] && [ -w "$BACKUPS_DIR" ] || die "backup destination is unavailable"
  [ -d "$RUNS_DIR" ] && [ -w "$RUNS_DIR" ] || die "run destination is unavailable"
  [[ "$DRAIN_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] && (( DRAIN_TIMEOUT_SECONDS <= 3600 )) || die "DRAIN_TIMEOUT_SECONDS must be between 1 and 3600"
  [[ "$DRAIN_POLL_SECONDS" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "DRAIN_POLL_SECONDS must be non-negative"
  local backup_required staging_required backup_available staging_available
  backup_required=$(( $(size_kib "$LIVE_APP") + $(size_kib "$LIVE_BIN") + $(size_kib "$DB") + $(size_kib "$SERVE_HUB") + SAFETY_KIB ))
  staging_required=$(( $(size_kib "$SOURCE_APP") + SAFETY_KIB ))
  backup_available="$(available_kib "$BACKUPS_DIR")"
  staging_available="$(available_kib "$LIVE_APP")"
  [[ "$backup_available" =~ ^[0-9]+$ ]] && (( backup_available >= backup_required )) || die "insufficient backup filesystem space: need=${backup_required}KiB available=${backup_available:-unknown}KiB"
  [[ "$staging_available" =~ ^[0-9]+$ ]] && (( staging_available >= staging_required )) || die "insufficient live-app filesystem space: need=${staging_required}KiB available=${staging_available:-unknown}KiB"
  local before
  before="$(health)" || die "backend unhealthy before drain"
  ORIGINAL_RUN_ID="$(printf '%s' "$before" | jq -er .runID)" || die "backend run identity missing"
  ORIGINAL_RUN_COUNT="$(service_run_count)" || die "canonical backend service run count missing"
  [[ "$ORIGINAL_RUN_COUNT" =~ ^[0-9]+$ ]] || die "backend run count is invalid"
  [ "$(printf '%s' "$before" | jq -r '.accepting // false')" = true ] || die "backend is not accepting before drain"
  if (( DRY_RUN == 0 )); then log "preflight=complete run=$ORIGINAL_RUN_ID"; fi
}

backup() {
  mkdir "$BACKUP"
  state backup running
  cp "$LIVE_BIN" "$BACKUP/opencodex"
  ditto "$LIVE_APP" "$BACKUP/OpencodeX.app"
  cp "$SERVE_HUB" "$BACKUP/serve-hub.sh"
  sqlite3 "$DB" ".backup '$BACKUP/opencode.db'"
  state backup complete
}

begin_drain() {
  state drain running
  curl -fsS --max-time 5 -u "$AUTH" -H 'Content-Type: application/json' \
    -d "{\"expectedRunID\":\"$ORIGINAL_RUN_ID\"}" "http://127.0.0.1:4096/global/drain/begin"
  DRAIN_ACTIVE=1
  local drain='' deadline
  deadline=$((SECONDS + DRAIN_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    drain="$(curl -fsS --max-time 5 -u "$AUTH" "http://127.0.0.1:4096/global/drain" 2>/dev/null || true)"
    [ "$(printf '%s' "$drain" | jq -r '.ready // false' 2>/dev/null || true)" = true ] && break
    sleep "$DRAIN_POLL_SECONDS"
  done
  [ -n "$drain" ] && [ "$(printf '%s' "$drain" | jq -r '.ready // false')" = true ] || die "drain did not become ready"
  state drain ready
}

rollback() {
  ROLLBACK_DONE=1
  state rollback running
  [ -f "$BACKUP/opencodex" ] || die "rollback binary backup missing"
  [ -d "$BACKUP/OpencodeX.app" ] || die "rollback app backup missing"
  if [ -d "$LIVE_APP" ]; then mv "$LIVE_APP" "${LIVE_APP}.failed-$RUN_ID"; fi
  if [ -d "$OLD_APP" ]; then mv "$OLD_APP" "$LIVE_APP"; else ditto "$BACKUP/OpencodeX.app" "$LIVE_APP"; fi
  install -m 755 "$BACKUP/opencodex" "$LIVE_BIN"
  restart_backend
  local restored
  restored="$(health)"
  [ "$(printf '%s' "$restored" | jq -r '.accepting // false')" = true ] || die "rollback did not restore accepting backend"
  state rollback complete
  state complete failed
}

retain() {
  local marker dir kind count
  for kind in known-good failed; do
    count=0
    while IFS= read -r dir; do
      [ -d "$dir" ] || continue
      [ "$dir" = "$BACKUP" ] && continue
      marker="$(grep '^state=' "$dir/manifest" 2>/dev/null | tail -n 1 || true)"
      [ "$marker" = "state=$kind" ] || continue
      count=$((count + 1))
      (( count <= 3 )) || rm -rf -- "$dir"
    done < <(ls -dt "$BACKUPS_DIR"/deploy-* 2>/dev/null || true)
  done
}

deploy() {
  preflight
  backup
  begin_drain
  CUTOVER_STARTED=1
  state cutover running
  if ! ditto "$SOURCE_APP" "$NEW_APP" || ! mv "$LIVE_APP" "$OLD_APP" || ! mv "$NEW_APP" "$LIVE_APP" || ! install -m 755 "$SOURCE_BIN" "$LIVE_BIN" || ! restart_backend; then
    rollback
    return 1
  fi
  local after=''
  for ((_=0; _<HEALTH_ATTEMPTS; _++)); do
    after="$(health 2>/dev/null || true)"
    [ "$(printf '%s' "$after" | jq -r '.healthy // false' 2>/dev/null || true)" = true ] && break
    sleep "$SLEEP_SECONDS"
  done
  if [ -z "$after" ] || [ "$(printf '%s' "$after" | jq -r '.accepting // false')" != true ] || [ "$(printf '%s' "$after" | jq -er .runID)" = "$ORIGINAL_RUN_ID" ] || [ "$(service_run_count)" -ne $((ORIGINAL_RUN_COUNT + 1)) ]; then rollback; return 1; fi
  sleep "$SLEEP_SECONDS"
  after="$(health)" || { rollback; return 1; }
  if [ "$(service_run_count)" -ne $((ORIGINAL_RUN_COUNT + 1)) ] || [ "$(printf '%s' "$after" | jq -er .runID)" = "$ORIGINAL_RUN_ID" ]; then rollback; return 1; fi
  if ! curl -fsS --max-time 30 -u "$AUTH" -H 'Content-Type: application/json' -d "{\"expectedRunID\":\"$(printf '%s' "$after" | jq -er .runID)\"}" "http://127.0.0.1:4096/global/drain/replay"; then
    rollback
    return 1
  fi
  DRAIN_ACTIVE=0
  if ! rm -rf -- "$OLD_APP"; then rollback; return 1; fi
  state complete known-good
  retain
}

main() {
  case "${1:-}" in
    --dry-run) DRY_RUN=1 ;;
    --prune-retention) retain; return ;;
    '') ;;
    *) usage; return 64 ;;
  esac
  [ -n "$DEPLOYMENT_ID" ] || { usage; return 64; }
  if (( DRY_RUN )); then preflight; printf 'dry-run: validation succeeded\n'; return; fi
  acquire_lock
  trap on_exit EXIT
  trap on_signal INT TERM HUP
  consume_deployment_id
  initialize_run
  deploy
}

if [[ "${DEPLOY_CANONICAL_LIBRARY:-0}" != 1 ]]; then main "$@"; fi
