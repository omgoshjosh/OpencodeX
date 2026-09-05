#!/bin/bash
# One-command local build + deploy that keeps the OpencodeX server, GUI and TUI
# in lockstep.  https://github.com/ecgreen/OpencodeX/issues/42
#
# The attach-time version gate is exact-match by design: a GUI whose sidecar
# stamp differs from the server's is refused.  Lockstep is therefore already
# ENFORCED; what was missing was a way to PRODUCE a matched set.  This script
# resolves one ref to one commit, computes one stamp, and pins that stamp into
# every artifact via OPENCODE_VERSION/OPENCODE_CHANNEL, which `packages/script`
# honours ahead of anything it would otherwise derive.
#
# It does not reimplement the server cutover.  `~/.opencode/safe-cutover-v2.sh`
# already owns the transactional binary swap (backup, bootout, install,
# bootstrap, verify, probes, automatic recovery) and this script drives it.
# What this adds around it is the drain gate (`GET /global/restart-readiness`,
# the same signal `waitForRestartReadiness` in the SDK samples), the GUI half
# of the deploy, and the stamp-equality check that runs BEFORE anything
# installed is touched.
set -Eeuo pipefail

readonly SELF="oxd"
readonly TARGET="opencode-darwin-arm64"

BUILD_DIR="${OXD_BUILD_DIR:-$HOME/agents/worktrees/dogfood-stack}"
CUTOVER="${OXD_CUTOVER:-$HOME/.opencode/safe-cutover-v2.sh}"
CLI_INSTALL="${OXD_CLI_INSTALL:-$HOME/.opencode/bin/opencodex}"
GUI_INSTALL="${OXD_GUI_INSTALL:-/Applications/OpencodeX.app}"
STATE_DIR="${OXD_STATE_DIR:-$HOME/.opencode/oxd}"
PASS_FILE="${OXD_PASS_FILE:-$HOME/.opencode/serve.pass}"
PORT="${OXD_PORT:-4096}"

REF="${OXD_REF:-fork/dogfood/stack2}"
CHANNEL=""
STAMP=""
STAGE=""
ASSUME_YES=0
STAGE_ONLY=0
DRAIN_ONLY=0
DO_FETCH=1
SKIP_GUI=0
FORCE=0
QUIT_GUI=1
DRAIN=1
DRAIN_SAMPLES=10
DRAIN_INTERVAL=6
DRAIN_TIMEOUT=3600

usage() {
  cat <<EOF
usage: $SELF [REF] [options]

Builds the OpencodeX CLI binary (serve + TUI) and the GUI app from ONE commit
with ONE version stamp, then installs both and restarts the server drain-safe.

  REF                    git ref to deploy (default: $REF)

  --stage-only           build and verify stamp equality, install nothing
  --drain-only           only wait for the server to be safe to restart, then exit
  --out DIR              staging directory (default: $STATE_DIR/stage/<stamp>)
  --stamp S              use this exact version stamp instead of computing one
  --channel C            OPENCODE_CHANNEL (default: derived from REF)
  --build-dir DIR        checkout to build in (default: $BUILD_DIR)
  --no-fetch             do not 'git fetch' before resolving REF
  --skip-gui             deploy the CLI only (leaves the GUI out of lockstep)
  --yes                  do not ask for confirmation
  --force                redeploy even if this commit+stamp is already live

  --drain-samples N      consecutive idle readiness samples (default: $DRAIN_SAMPLES)
  --drain-interval S     seconds between samples (default: $DRAIN_INTERVAL)
  --drain-timeout S      give up waiting to drain after S seconds (default: $DRAIN_TIMEOUT)
  --no-drain             restart without waiting for the server to go idle
  --no-quit-gui          do not quit a running OpencodeX.app before replacing it

Day to day:  $SELF --yes
EOF
}

say() { printf '%s\n' "$*"; }
step() { printf '\n== %s\n' "$*"; }
die() { printf '%s: %s\n' "$SELF" "$*" >&2; exit 1; }

while [ "$#" -gt 0 ]; do
  case "$1" in
    -h | --help) usage; exit 0 ;;
    --stage-only) STAGE_ONLY=1 ;;
    --drain-only) DRAIN_ONLY=1 ;;
    --yes | -y) ASSUME_YES=1 ;;
    --force) FORCE=1 ;;
    --no-fetch) DO_FETCH=0 ;;
    --skip-gui) SKIP_GUI=1 ;;
    --no-drain) DRAIN=0 ;;
    --no-quit-gui) QUIT_GUI=0 ;;
    --out) STAGE="${2:?--out needs a directory}"; shift ;;
    --stamp) STAMP="${2:?--stamp needs a value}"; shift ;;
    --channel) CHANNEL="${2:?--channel needs a value}"; shift ;;
    --build-dir) BUILD_DIR="${2:?--build-dir needs a directory}"; shift ;;
    --drain-samples) DRAIN_SAMPLES="${2:?}"; shift ;;
    --drain-interval) DRAIN_INTERVAL="${2:?}"; shift ;;
    --drain-timeout) DRAIN_TIMEOUT="${2:?}"; shift ;;
    -*) die "unknown option $1 (try --help)" ;;
    *) REF="$1" ;;
  esac
  shift
done

# ---------------------------------------------------------------- preflight --
step "preflight"
for tool in bun git jq curl launchctl shasum ditto osascript; do
  command -v "$tool" >/dev/null || die "missing required tool: $tool"
done
[ -d "$BUILD_DIR/.git" ] || [ -f "$BUILD_DIR/.git" ] || die "not a checkout: $BUILD_DIR"
[ -x "$CUTOVER" ] || [ -r "$CUTOVER" ] || die "missing cutover script: $CUTOVER"
[ -s "$PASS_FILE" ] || die "missing server password file: $PASS_FILE"
[ "$STAGE_ONLY" = 1 ] || [ -x "$CLI_INSTALL" ] || die "no installed CLI at $CLI_INSTALL"

FREE_GB="$(df -g / | awk 'NR==2 {print $4}')"
[ "${FREE_GB:-0}" -ge 10 ] || die "only ${FREE_GB}GB free; hard build stop is 10GB"
say "free disk       ${FREE_GB}GB"

AUTH=(-u "opencode:$(cat "$PASS_FILE")")
api() { curl --fail --silent --max-time "${2:-10}" "${AUTH[@]}" "http://127.0.0.1:$PORT$1"; }
live_version() { api /global/health 5 2>/dev/null | jq -r '.version // empty'; }

SERVER_VERSION="$(live_version || true)"
say "server          ${SERVER_VERSION:-<not responding>}"

# The server publishes its own restart safety on /global/restart-readiness:
# `ready` plus the named blockers that are holding work open.  Requiring N
# consecutive idle samples is the same shape as waitForRestartReadiness in the
# SDK (defaults there: 10 samples, 6s apart), which is what keeps a restart
# from landing in the gap between two turns of the same session.
wait_for_drain() {
  step "drain"
  if [ "$DRAIN" = 0 ]; then
    say "SKIPPED (--no-drain): an in-flight turn can be cut off by this restart"
    return 0
  fi
  say "waiting for $DRAIN_SAMPLES consecutive idle samples every ${DRAIN_INTERVAL}s (timeout ${DRAIN_TIMEOUT}s)"
  local deadline consecutive sample
  deadline=$(( $(date +%s) + DRAIN_TIMEOUT ))
  consecutive=0
  while [ "$consecutive" -lt "$DRAIN_SAMPLES" ]; do
    [ "$(date +%s)" -lt "$deadline" ] ||
      die "server never drained within ${DRAIN_TIMEOUT}s — nothing was installed"
    sample="$(api /global/restart-readiness 10 || true)"
    if [ -z "$sample" ]; then
      consecutive=0
      say "  waiting for drain: readiness check failed (server not responding)"
    elif [ "$(printf '%s' "$sample" | jq -r '.ready')" = "true" ]; then
      consecutive=$((consecutive + 1))
      say "  waiting for drain: idle $consecutive/$DRAIN_SAMPLES"
    else
      consecutive=0
      say "  waiting for drain: busy — $(printf '%s' "$sample" | jq -r '[.blockers|to_entries[]|select(.value)|.key]|join(", ")')"
    fi
    [ "$consecutive" -ge "$DRAIN_SAMPLES" ] || sleep "$DRAIN_INTERVAL"
  done
  say "server is drained"
}

if [ "$DRAIN_ONLY" = 1 ]; then
  wait_for_drain
  exit 0
fi

# ------------------------------------------------------------------ resolve --
step "resolve"
cd "$BUILD_DIR"
DIRTY="$(git status --porcelain)"
[ -z "$DIRTY" ] || {
  printf '%s\n' "$DIRTY" >&2
  die "checkout is dirty: $BUILD_DIR — commit, stash or clean it first"
}
[ "$DO_FETCH" = 0 ] || git fetch --quiet --prune fork 2>/dev/null || git fetch --quiet --prune --all

SHA="$(git rev-parse --verify --quiet "${REF}^{commit}")" || die "cannot resolve ref: $REF"
SUBJECT="$(git log -1 --format=%s "$SHA")"

if [ -z "$CHANNEL" ]; then
  CHANNEL="${REF#refs/remotes/}"
  CHANNEL="${CHANNEL#refs/heads/}"
  CHANNEL="${CHANNEL#fork/}"
  CHANNEL="${CHANNEL#origin/}"
  CHANNEL="${CHANNEL#upstream/}"
fi
case "$CHANNEL" in
  "" | *[!a-zA-Z0-9/._-]* ) die "cannot derive a channel from '$REF'; pass --channel" ;;
esac
[ "$CHANNEL" != "$SHA" ] || die "REF is a raw commit; pass --channel"

[ -n "$STAMP" ] || STAMP="0.0.0-${CHANNEL}-$(date -u +%Y%m%d%H%M)"
case "$STAMP" in
  0.0.0-*) : ;;
  *) die "stamp must start with 0.0.0- or the build treats it as a release channel: $STAMP" ;;
esac
SAFE_STAMP="${STAMP//\//-}"

[ -n "$STAGE" ] || STAGE="$STATE_DIR/stage/$SAFE_STAMP"
BACKUP_DIR="$STATE_DIR/backups/$SAFE_STAMP"
LAST="$STATE_DIR/last-deploy.json"
CUTOVER_NONCE="oxd-$SAFE_STAMP"
STAGED_CLI="$STAGE/opencodex"
STAGED_APP="$STAGE/OpencodeX.app"

installed_gui_version() {
  local stamp="$GUI_INSTALL/Contents/Resources/sidecar/version.json"
  [ -r "$stamp" ] || return 0
  jq -r '.version // empty' "$stamp" 2>/dev/null
}
GUI_VERSION="$(installed_gui_version)"

# ---------------------------------------------------------------- the plan --
step "plan"
cat <<EOF
  ref             $REF
  commit          $SHA
                  $SUBJECT
  channel         $CHANNEL
  stamp           $STAMP
  build in        $BUILD_DIR
  stage in        $STAGE

  CLI   $CLI_INSTALL
        ${SERVER_VERSION:-<server not responding>}  ->  $STAMP
  GUI   $GUI_INSTALL
        ${GUI_VERSION:-<unstamped>}  ->  $([ "$SKIP_GUI" = 1 ] && echo "(skipped)" || echo "$STAMP")
EOF

if [ "$FORCE" = 0 ] && [ "$STAGE_ONLY" = 0 ] && [ -r "$LAST" ]; then
  if [ "$(jq -r '.commit // empty' "$LAST")" = "$SHA" ] &&
     [ -n "$SERVER_VERSION" ] &&
     [ "$(jq -r '.stamp // empty' "$LAST")" = "$SERVER_VERSION" ] &&
     { [ "$SKIP_GUI" = 1 ] || [ "$GUI_VERSION" = "$SERVER_VERSION" ]; }; then
    say ""
    say "LOCKSTEP PASS  $SERVER_VERSION  server=GUI=TUI  (already deployed from $SHA; --force to rebuild)"
    exit 0
  fi
fi

if [ "$ASSUME_YES" = 0 ]; then
  printf '\nproceed? [y/N] '
  if [ -r /dev/tty ] && [ -t 1 ]; then
    read -r reply </dev/tty || reply=""
  else
    read -r reply || reply=""
  fi
  case "$reply" in [yY] | [yY][eE][sS]) : ;; *) printf '\n'; die "aborted" ;; esac
fi

# ------------------------------------------------------------------- build --
step "build"
mkdir -p "$STAGE"
rm -rf "$STAGED_CLI" "$STAGED_APP"

CURRENT_SHA="$(git rev-parse HEAD)"
if [ "$CURRENT_SHA" != "$SHA" ]; then
  say "checking out $SHA"
  git checkout --detach --quiet "$SHA"
fi

export OPENCODE_VERSION="$STAMP"
export OPENCODE_CHANNEL="$CHANNEL"

# The CLI binary is serve + TUI in one artifact.  build.ts starts with
# `rm -rf dist`, so the CLI has to be copied out of dist before the coordinator
# build reuses the same directory.
say "building CLI (serve + TUI)"
bun run --cwd packages/opencode build --single
install -m 755 "packages/opencode/dist/$TARGET/bin/opencode" "$STAGED_CLI"

if [ "$SKIP_GUI" = 0 ]; then
  say "building GUI coordinator sidecar"
  bun run --cwd packages/opencode build --single --gui-coordinator --skip-install

  say "packaging GUI app"
  # copy-sidecar.ts prefers this over dist/<target>/package.json; both carry the
  # same stamp, and setting it explicitly means a stale dist cannot mis-stamp
  # the bundle the version gate reads.
  OPENCODEX_GUI_SIDECAR_VERSION="$STAMP" bun run --cwd packages/gui package
  BUILT_APP=""
  for candidate in packages/gui/release/mac-arm64/OpencodeX.app packages/gui/release/mac/OpencodeX.app; do
    [ -d "$candidate" ] && { BUILT_APP="$candidate"; break; }
  done
  [ -n "$BUILT_APP" ] || die "electron-builder produced no OpencodeX.app under packages/gui/release"
  ditto "$BUILT_APP" "$STAGED_APP"
fi

# --------------------------------------------------------- lockstep gate ---
# Nothing installed has been touched yet.  If the artifacts disagree, stop here.
step "verify artifacts"
CLI_STAMP="$("$STAGED_CLI" --version 2>/dev/null || true)"
say "CLI  $CLI_STAMP"
[ "$CLI_STAMP" = "$STAMP" ] || die "LOCKSTEP FAIL  CLI stamped '$CLI_STAMP', expected '$STAMP'"

if [ "$SKIP_GUI" = 0 ]; then
  APP_STAMP="$(jq -r '.version // empty' "$STAGED_APP/Contents/Resources/sidecar/version.json" 2>/dev/null || true)"
  say "GUI  $APP_STAMP"
  [ "$APP_STAMP" = "$STAMP" ] || die "LOCKSTEP FAIL  GUI sidecar stamped '$APP_STAMP', expected '$STAMP'"
  [ -x "$STAGED_APP/Contents/Resources/sidecar/opencode-gui-coordinator" ] ||
    die "packaged app has no sidecar coordinator binary"
fi

CLI_HASH="$(shasum -a 256 "$STAGED_CLI" | cut -d ' ' -f 1)"
say "artifacts agree on $STAMP"

if [ "$STAGE_ONLY" = 1 ]; then
  step "stage only"
  say "  CLI  $STAGED_CLI  (sha256 $CLI_HASH)"
  [ "$SKIP_GUI" = 1 ] || say "  GUI  $STAGED_APP"
  say ""
  say "LOCKSTEP PASS (staged)  $STAMP  — nothing installed; re-run without --stage-only to deploy"
  exit 0
fi

# ------------------------------------------------------------------- drain --
wait_for_drain

# ----------------------------------------------------------------- install --
GUI_BACKUP=""
if [ "$SKIP_GUI" = 0 ]; then
  step "install GUI"
  mkdir -p "$BACKUP_DIR"
  if [ -d "$GUI_INSTALL" ]; then
    GUI_BACKUP="$BACKUP_DIR/OpencodeX.app"
    rm -rf "$GUI_BACKUP"
    ditto "$GUI_INSTALL" "$GUI_BACKUP"
    say "backed up  $GUI_INSTALL -> $GUI_BACKUP"
  fi
  if [ "$QUIT_GUI" = 1 ] && pgrep -f "$GUI_INSTALL/Contents/MacOS/" >/dev/null 2>&1; then
    say "quitting running OpencodeX.app"
    osascript -e 'quit app "OpencodeX"' >/dev/null 2>&1 || true
    for _ in $(seq 1 15); do
      pgrep -f "$GUI_INSTALL/Contents/MacOS/" >/dev/null 2>&1 || break
      sleep 1
    done
    pgrep -f "$GUI_INSTALL/Contents/MacOS/" >/dev/null 2>&1 &&
      say "  still running; replacing the bundle anyway — relaunch it when the deploy finishes"
  fi
  rm -rf "$GUI_INSTALL.oxd-incoming"
  ditto "$STAGED_APP" "$GUI_INSTALL.oxd-incoming"
  rm -rf "$GUI_INSTALL"
  mv "$GUI_INSTALL.oxd-incoming" "$GUI_INSTALL"
  say "installed  $GUI_INSTALL"
fi

step "install CLI and restart server"
say "handing off to $(basename "$CUTOVER") (nonce $CUTOVER_NONCE)"
CUTOVER_NONCE="$CUTOVER_NONCE" /bin/bash "$CUTOVER" "$STAGED_CLI" "$CLI_HASH" "$STAMP"
CUTOVER_ROOT="$HOME/.opencode/cutovers/$CUTOVER_NONCE"

# ------------------------------------------------------------------ verify --
step "verify deployment"
FINAL_SERVER=""
for _ in $(seq 1 30); do
  FINAL_SERVER="$(live_version || true)"
  [ -n "$FINAL_SERVER" ] && break
  sleep 2
done
FINAL_GUI="$(installed_gui_version)"
say "server  ${FINAL_SERVER:-<not responding>}"
[ "$SKIP_GUI" = 1 ] || say "GUI     ${FINAL_GUI:-<unstamped>}"

OK=1
[ "$FINAL_SERVER" = "$STAMP" ] || OK=0
[ "$SKIP_GUI" = 1 ] || [ "$FINAL_GUI" = "$STAMP" ] || OK=0

# ---------------------------------------------------------------- rollback --
step "rollback"
say "  CLI + server:  /bin/bash $HOME/.opencode/cutover-recover-v2.sh $CUTOVER_ROOT"
if [ -n "$GUI_BACKUP" ]; then
  say "  GUI:           rm -rf $GUI_INSTALL && ditto $GUI_BACKUP $GUI_INSTALL"
else
  say "  GUI:           (no previous app bundle was installed; remove with: rm -rf $GUI_INSTALL)"
fi

say ""
if [ "$OK" = 1 ]; then
  mkdir -p "$STATE_DIR"
  jq -n --arg commit "$SHA" --arg stamp "$STAMP" --arg ref "$REF" --arg channel "$CHANNEL" \
    --arg cutover "$CUTOVER_ROOT" --arg guiBackup "$GUI_BACKUP" --arg at "$(date -u '+%FT%TZ')" \
    '{commit:$commit,stamp:$stamp,ref:$ref,channel:$channel,cutover:$cutover,guiBackup:$guiBackup,at:$at}' \
    > "$LAST.tmp" && mv "$LAST.tmp" "$LAST"
  say "LOCKSTEP PASS  $STAMP  server=GUI=TUI  from $SHA"
  [ "$SKIP_GUI" = 1 ] || say "relaunch OpencodeX.app to pick up the new GUI"
  exit 0
fi
say "LOCKSTEP FAIL  server='${FINAL_SERVER:-none}' gui='${FINAL_GUI:-none}' expected='$STAMP' — roll back with the commands above"
exit 1
