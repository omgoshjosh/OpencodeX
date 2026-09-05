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
RECOVER="${OXD_RECOVER:-$HOME/.opencode/cutover-recover-v2.sh}"
CLI_INSTALL="${OXD_CLI_INSTALL:-$HOME/.opencode/bin/opencodex}"
GUI_INSTALL="${OXD_GUI_INSTALL:-/Applications/OpencodeX.app}"
STATE_DIR="${OXD_STATE_DIR:-$HOME/.opencode/oxd}"
PASS_FILE="${OXD_PASS_FILE:-$HOME/.opencode/serve.pass}"
PORT="${OXD_PORT:-4096}"

# A deploy costs roughly 3.7GB that nothing else prunes: the cutover root keeps
# a full database snapshot (~3GB), plus the staged binary, the staged app and
# the app backup.  Refuse to start unless that fits and still clears the
# workspace's 10GB hard build stop afterwards.
readonly DEPLOY_COST_GB=4
readonly HARD_STOP_GB=10
readonly KEEP_GENERATIONS=3

REF="${OXD_REF:-fork/dogfood/stack2}"
REF_EXPLICIT=0
STAMP_SHA=""
CHANNEL=""
STAMP=""
STAGE=""
ASSUME_YES=0
STAGE_ONLY=0
DRAIN_ONLY=0
DO_FETCH=1
SKIP_GUI=0
GUI_ONLY=0
FORCE=0
QUIT_GUI=1
DRAIN=1
DRAIN_SAMPLES=10
DRAIN_INTERVAL=6
DRAIN_TIMEOUT=900
DRAIN_IGNORE=""

usage() {
  cat <<EOF
usage: $SELF [REF] [options]

Builds the OpencodeX CLI binary (serve + TUI) and the GUI app from ONE commit
with ONE version stamp, then installs both and restarts the server drain-safe.

  REF                    git ref to deploy (default: $REF)

  --gui-only             rebuild and install ONLY the GUI, to match the server
                         that is already running: no CLI build, no drain, no
                         cutover.  REF and --stamp both default to whatever the
                         live server reports, so the GUI is rebuilt at the
                         commit the server was built from.
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
  --drain-ignore A,B     treat these readiness blockers as idle, e.g. 'swarms'
                         for swarms parked in approval_needed/blocked
  --no-drain             restart without waiting for the server to go idle
  --no-quit-gui          do not quit a running OpencodeX.app before replacing it

Day to day:      $SELF --yes
After a cutover: $SELF --gui-only --yes
EOF
}

say() { printf '%s\n' "$*"; }
step() { printf '\n== %s\n' "$*"; }
die() { printf '%s: %s\n' "$SELF" "$*" >&2; exit 1; }
positive_int() {
  case "$2" in "" | *[!0-9]* | 0) die "$1 must be a positive integer, got '$2'" ;; esac
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    -h | --help) usage; exit 0 ;;
    --stage-only) STAGE_ONLY=1 ;;
    --drain-only) DRAIN_ONLY=1 ;;
    --gui-only) GUI_ONLY=1 ;;
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
    --drain-ignore) DRAIN_IGNORE="${2:?--drain-ignore needs blocker names}"; shift ;;
    --drain-samples) DRAIN_SAMPLES="${2:?}"; shift ;;
    --drain-interval) DRAIN_INTERVAL="${2:?}"; shift ;;
    --drain-timeout) DRAIN_TIMEOUT="${2:?}"; shift ;;
    -*) die "unknown option $1 (try --help)" ;;
    *) REF="$1"; REF_EXPLICIT=1 ;;
  esac
  shift
done

if [ "$GUI_ONLY" = 1 ]; then
  [ "$SKIP_GUI" = 0 ] || die "--gui-only and --skip-gui ask for opposite halves of the deploy"
  [ "$DRAIN_ONLY" = 0 ] || die "--gui-only does not restart the server, so there is nothing to drain for; drop one of --gui-only/--drain-only"
fi

positive_int --drain-samples "$DRAIN_SAMPLES"
positive_int --drain-interval "$DRAIN_INTERVAL"
positive_int --drain-timeout "$DRAIN_TIMEOUT"
# A relative --out would otherwise land inside the checkout once we cd into it,
# dirtying the tree the next run refuses to build from.
case "$STAGE" in "" | /*) : ;; *) STAGE="$PWD/$STAGE" ;; esac

# ------------------------------------------------------------ exit handling --
# safe-cutover-v2.sh exits nonzero whenever it fails, INCLUDING after it has
# successfully recovered the server to the old binary.  Under errexit that
# would otherwise terminate this script at the handoff, before the rollback
# instructions are ever printed — exactly when they are needed most.
ROLLBACK_ARMED=0
ORIGINAL_HEAD=""
CUTOVER_ROOT=""
GUI_BACKUP=""

print_rollback() {
  step "rollback"
  if [ -n "$CUTOVER_ROOT" ] && [ -f "$CUTOVER_ROOT/manifest.json" ]; then
    say "  CLI + server:  /bin/bash $RECOVER $CUTOVER_ROOT"
  else
    say "  CLI + server:  nothing to undo (the binary swap never started)"
  fi
  if [ -n "$GUI_BACKUP" ] && [ -d "$GUI_BACKUP" ]; then
    say "  GUI:           rm -rf $GUI_INSTALL && ditto $GUI_BACKUP $GUI_INSTALL"
  else
    say "  GUI:           nothing to undo (no bundle was replaced)"
  fi
}

on_exit() {
  local rc=$?
  trap - EXIT
  # Restore the shared build checkout to whatever ref it was on; other sessions
  # hold a claim on it and did not ask for a detached HEAD.
  if [ -n "$ORIGINAL_HEAD" ]; then
    git -C "$BUILD_DIR" checkout --quiet --force "$ORIGINAL_HEAD" 2>/dev/null || true
  fi
  [ "$rc" = 0 ] || [ "$ROLLBACK_ARMED" = 0 ] || print_rollback
  exit "$rc"
}
trap on_exit EXIT

# ---------------------------------------------------------------- preflight --
step "preflight"
required=(git jq curl)
[ "$DRAIN_ONLY" = 1 ] || required+=(bun shasum ditto osascript launchctl)
for tool in "${required[@]}"; do
  command -v "$tool" >/dev/null || die "missing required tool: $tool"
done
[ -s "$PASS_FILE" ] || die "missing server password file: $PASS_FILE"

SERVE_PASS="$(cat "$PASS_FILE")"
# Credentials go through a curl config on stdin rather than argv, so the server
# password never shows up in `ps`.
api() {
  printf 'user = "opencode:%s"\n' "$SERVE_PASS" |
    curl --fail --silent --max-time "${2:-10}" -K - "http://127.0.0.1:$PORT$1"
}
live_version() { api /global/health 5 2>/dev/null | jq -r '.version // empty'; }

# The server publishes its own restart safety on /global/restart-readiness:
# `ready` plus the named blockers holding work open.  Requiring N consecutive
# idle samples is the same shape as waitForRestartReadiness in the SDK
# (defaults there: 10 samples, 6s apart), which is what keeps a restart from
# landing in the gap between two turns of the same session.
wait_for_drain() {
  step "drain"
  if [ "$DRAIN" = 0 ]; then
    say "SKIPPED (--no-drain): an in-flight turn can be cut off by this restart"
    return 0
  fi
  say "waiting for $DRAIN_SAMPLES consecutive idle samples every ${DRAIN_INTERVAL}s (timeout ${DRAIN_TIMEOUT}s)"
  [ -z "$DRAIN_IGNORE" ] || say "ignoring blockers: $DRAIN_IGNORE"
  local deadline consecutive sample blockers
  deadline=$(( $(date +%s) + DRAIN_TIMEOUT ))
  consecutive=0
  while [ "$consecutive" -lt "$DRAIN_SAMPLES" ]; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      say "  still blocked by: ${blockers:-unknown}"
      die "server never drained within ${DRAIN_TIMEOUT}s — nothing was installed.
    Wait for the blockers to clear, raise --drain-timeout, exempt a blocker with
    --drain-ignore (e.g. 'swarms' for swarms parked awaiting a human), or accept
    cutting off in-flight work with --no-drain."
    fi
    sample="$(api /global/restart-readiness 10 || true)"
    if [ -z "$sample" ]; then
      blockers="readiness check failed (server not responding)"
      consecutive=0
      say "  waiting for drain: $blockers"
    else
      blockers="$(printf '%s' "$sample" |
        jq -r --arg ignore "$DRAIN_IGNORE" \
          '($ignore | split(",") | map(select(length > 0))) as $skip
           | [.blockers | to_entries[] | select(.value) | .key | select(. as $k | $skip | index($k) | not)]
           | join(", ")')"
      if [ -z "$blockers" ]; then
        consecutive=$((consecutive + 1))
        say "  waiting for drain: idle $consecutive/$DRAIN_SAMPLES"
      else
        consecutive=0
        say "  waiting for drain: busy — $blockers"
      fi
    fi
    [ "$consecutive" -ge "$DRAIN_SAMPLES" ] || sleep "$DRAIN_INTERVAL"
  done
  say "server is drained"
}

SERVER_VERSION="$(live_version || true)"
say "server          ${SERVER_VERSION:-<not responding>}"

# ------------------------------------------------------ gui-only targeting --
# The operator cuts the server over several times a day through
# ~/.opencode/safe-cutover-v2.sh, which is handed a stamp of the shape
#   0.0.0-<channel, / replaced by ->-<7-char short sha>-<UTC YYYYMMDDHHMM>
# (see the EXPECTED_VERSION assignments in ~/.opencode/safe-cutover-*.sh; the
# cutover consumes that stamp, it does not derive it).  Nobody rebuilds the GUI
# on those cutovers and the attach gate is exact-match, so the GUI is refused
# after every one.  --gui-only repairs exactly that: read the stamp the server
# is actually running and rebuild the GUI half at the commit it names.  The
# default stamp built at the bottom of "resolve" writes this same format, and
# this is the parser that reads it back — keep the two in step.
if [ "$GUI_ONLY" = 1 ]; then
  [ -n "$STAMP" ] || [ -n "$SERVER_VERSION" ] ||
    die "--gui-only targets the running server, but it is not responding on port $PORT; pass REF and --stamp explicitly"
  [ -n "$STAMP" ] || STAMP="$SERVER_VERSION"
  # The channel may itself contain dashes (dogfood/safe-cutover -> two fields),
  # so peel the known-shaped tail off the right instead of counting from the
  # left: the timestamp is the last field and the short sha the one before it.
  STAMP_BODY="${STAMP#0.0.0-}"
  STAMP_TS="${STAMP_BODY##*-}"
  STAMP_HEAD="${STAMP_BODY%-*}"
  STAMP_SHA="${STAMP_HEAD##*-}"
  case "$STAMP_TS" in "" | *[!0-9]*) STAMP_SHA="" ;; esac
  case "$STAMP_SHA" in "" | *[!0-9a-fA-F]*) STAMP_SHA="" ;; esac
  # No dash left in the head means the stamp carried no channel, so what looks
  # like a sha is really the channel and there is nothing to build at.
  [ "$STAMP_HEAD" != "$STAMP_SHA" ] || STAMP_SHA=""
  [ -n "$STAMP_SHA" ] ||
    die "cannot read a commit out of the stamp '$STAMP' (expected 0.0.0-<channel>-<short sha>-<UTC timestamp>); pass REF and --stamp explicitly"
  [ "$REF_EXPLICIT" = 1 ] || REF="$STAMP_SHA"
  # The channel recovered here has already had its slashes flattened, so a
  # gui-only rebuild sets OPENCODE_CHANNEL=dogfood-stack2 where the full run
  # that built the server set dogfood/stack2.  That is safe only because the
  # channel reaches the attach gate through the database filename, which
  # applies the same substitution (`InstallationChannel.replace(/[^a-zA-Z0-9._-]/g, "-")`
  # in packages/core/src/database/database.ts), so both spellings resolve to
  # opencode-dogfood-stack2.db and coordinatorDatabaseIdentity still matches.
  # If that sanitiser changes, this has to derive the unflattened channel.
  [ -n "$CHANNEL" ] || CHANNEL="${STAMP_HEAD%-*}"
fi

if [ "$DRAIN_ONLY" = 1 ]; then
  wait_for_drain
  exit 0
fi

[ -d "$BUILD_DIR/.git" ] || [ -f "$BUILD_DIR/.git" ] || die "not a checkout: $BUILD_DIR"
[ -r "$CUTOVER" ] || die "missing cutover script: $CUTOVER"
[ -r "$RECOVER" ] || die "missing recovery script: $RECOVER"
[ "$STAGE_ONLY" = 1 ] || [ -x "$CLI_INSTALL" ] || die "no installed CLI at $CLI_INSTALL"

FREE_GB="$(df -g / | awk 'NR==2 {print $4}')"
NEEDED_GB=$(( HARD_STOP_GB + DEPLOY_COST_GB ))
[ "${FREE_GB:-0}" -ge "$NEEDED_GB" ] ||
  die "only ${FREE_GB}GB free; a deploy consumes about ${DEPLOY_COST_GB}GB and must not cross the ${HARD_STOP_GB}GB hard build stop"
say "free disk       ${FREE_GB}GB"

# ------------------------------------------------------------------ resolve --
step "resolve"
cd "$BUILD_DIR"
DIRTY="$(git status --porcelain)"
[ -z "$DIRTY" ] || {
  printf '%s\n' "$DIRTY" >&2
  die "checkout is dirty: $BUILD_DIR — commit, stash or clean it first"
}
if [ "$DO_FETCH" = 1 ]; then
  git fetch --quiet --prune fork 2>/dev/null ||
    git fetch --quiet --prune --all 2>/dev/null ||
    say "fetch failed; resolving $REF from the local checkout"
fi

SHA="$(git rev-parse --verify --quiet "${REF}^{commit}")" || die "cannot resolve ref: $REF"
SUBJECT="$(git log -1 --format=%s "$SHA")"

# An explicit REF suppresses the ref default but not the stamp default, so
# `--gui-only <other-ref>` would ship a GUI built from one commit wearing the
# stamp of another.  The attach gate compares stamp strings only, so it would
# accept that bundle and never notice it is running different code.
if [ "$GUI_ONLY" = 1 ] && [ "$REF_EXPLICIT" = 1 ] && [ "${SHA#"$STAMP_SHA"}" = "$SHA" ]; then
  say "WARNING  building $REF ($(git rev-parse --short=7 "$SHA")) but stamping it '$STAMP', which names $STAMP_SHA."
  say "         The GUI will attach to the running server while containing different code."
  say "         Pass --stamp too, or drop $REF to build what the server actually runs."
fi

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
# A bare commit-ish makes a meaningless channel; a branch name is what the
# deployed stamp is supposed to identify.
if git rev-parse --verify --quiet "${CHANNEL}^{commit}" >/dev/null &&
   ! git show-ref --verify --quiet "refs/heads/$CHANNEL" &&
   ! git show-ref --verify --quiet "refs/remotes/fork/$CHANNEL"; then
  die "'$CHANNEL' is a commit-ish, not a branch; pass --channel"
fi

# This must produce the same shape the operator's cutovers already run on —
# 0.0.0-<channel, / replaced by ->-<7-char short sha>-<UTC YYYYMMDDHHMM> — both
# because the stamp is the only human-readable record of what a live server was
# built from, and because --gui-only parses the sha back out of it above.
# safe-cutover-v2.sh takes the stamp as an argument rather than deriving it; the
# convention is visible in the EXPECTED_VERSION lines of its pinned siblings,
# e.g. 0.0.0-dogfood-safe-cutover-32206ba-202608311126.
[ -n "$STAMP" ] || STAMP="0.0.0-${CHANNEL//\//-}-$(git rev-parse --short=7 "$SHA")-$(date -u +%Y%m%d%H%M)"
case "$STAMP" in
  0.0.0-*) : ;;
  *) die "stamp must start with 0.0.0- or the build treats it as a release channel: $STAMP" ;;
esac
SAFE_STAMP="${STAMP//\//-}"

[ -n "$STAGE" ] || STAGE="$STATE_DIR/stage/$SAFE_STAMP"
# Backups and the cutover root must never be shared between two runs: a second
# run in the same minute would otherwise overwrite the first run's backup with
# its own output, leaving a rollback point that rolls back to nothing.
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
BACKUP_DIR="$STATE_DIR/backups/$RUN_ID"
LAST="$STATE_DIR/last-deploy.json"
CUTOVER_NONCE="oxd-$SAFE_STAMP-$RUN_ID"
CUTOVER_ROOT="$HOME/.opencode/cutovers/$CUTOVER_NONCE"
STAGED_CLI="$STAGE/opencodex"
STAGED_APP="$STAGE/OpencodeX.app"

installed_gui_version() {
  local stamp="$GUI_INSTALL/Contents/Resources/sidecar/version.json"
  [ -r "$stamp" ] || return 0
  jq -r '.version // empty' "$stamp" 2>/dev/null
}
GUI_VERSION="$(installed_gui_version)"
lockstep_claim() {
  if [ "$GUI_ONLY" = 1 ]; then
    printf 'server=GUI=TUI (GUI rebuilt onto the running server; nothing restarted)'
  elif [ "$SKIP_GUI" = 1 ]; then
    printf 'server=TUI (GUI skipped, still at %s)' "${GUI_VERSION:-<unstamped>}"
  else
    printf 'server=GUI=TUI'
  fi
}

# ---------------------------------------------------------------- the plan --
step "plan"
cat <<EOF
  mode            $([ "$GUI_ONLY" = 1 ] && echo "GUI only (resolved from the running server)" || echo "CLI + GUI + server cutover")
  ref             $REF
  commit          $SHA
                  $SUBJECT
  channel         $CHANNEL
  stamp           $STAMP
  build in        $BUILD_DIR
  stage in        $STAGE

  CLI   $CLI_INSTALL
        ${SERVER_VERSION:-<server not responding>}  ->  $([ "$GUI_ONLY" = 1 ] && echo "(unchanged; the server is not restarted)" || echo "$STAMP")
  GUI   $GUI_INSTALL
        ${GUI_VERSION:-<unstamped>}  ->  $([ "$SKIP_GUI" = 1 ] && echo "(skipped)" || echo "$STAMP")
EOF

# A GUI-only run never writes last-deploy.json, so its "already done" signal is
# the installed bundle matching the server, not that record.  Both halves have
# to be checked: with an explicit --stamp that the server does not run, a GUI
# matching that stamp is precisely the mismatch this command exists to fix, and
# claiming lockstep for it would turn the LOCKSTEP FAIL the previous run
# correctly reported into a PASS on the next one.
if [ "$GUI_ONLY" = 1 ] && [ "$FORCE" = 0 ] && [ "$STAGE_ONLY" = 0 ] &&
   [ "$GUI_VERSION" = "$STAMP" ] && [ "$SERVER_VERSION" = "$STAMP" ]; then
  say ""
  say "LOCKSTEP PASS  $STAMP  $(lockstep_claim)  (GUI already matches; --force to rebuild)"
  exit 0
fi

if [ "$GUI_ONLY" = 0 ] && [ "$FORCE" = 0 ] && [ "$STAGE_ONLY" = 0 ] && [ -r "$LAST" ]; then
  if [ "$(jq -r '.commit // empty' "$LAST")" = "$SHA" ] &&
     [ -n "$SERVER_VERSION" ] &&
     [ "$(jq -r '.stamp // empty' "$LAST")" = "$SERVER_VERSION" ] &&
     { [ "$SKIP_GUI" = 1 ] || [ "$GUI_VERSION" = "$SERVER_VERSION" ]; }; then
    say ""
    say "LOCKSTEP PASS  $SERVER_VERSION  $(lockstep_claim)  (already deployed from $SHA; --force to rebuild)"
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

ORIGINAL_HEAD="$(git symbolic-ref --quiet --short HEAD || git rev-parse HEAD)"
if [ "$(git rev-parse HEAD)" != "$SHA" ]; then
  say "checking out $SHA (restoring $ORIGINAL_HEAD on exit)"
  git checkout --detach --quiet "$SHA"
fi

export OPENCODE_VERSION="$STAMP"
export OPENCODE_CHANNEL="$CHANNEL"
# copy-sidecar.ts checks OPENCODEX_GUI_SIDECAR *before* the binary this build
# produces. An operator with that exported from a dev workflow would otherwise
# ship an arbitrary coordinator under a freshly minted stamp.
unset OPENCODEX_GUI_SIDECAR OPENCODEX_GUI_SIDECAR_TARGET

# The CLI binary is serve + TUI in one artifact.  build.ts starts with
# `rm -rf dist`, so the CLI has to be copied out of dist before the coordinator
# build reuses the same directory.
if [ "$GUI_ONLY" = 1 ]; then
  say "skipping the CLI build (--gui-only): the server already runs $STAMP"
else
  say "building CLI (serve + TUI)"
  bun run --cwd packages/opencode build --single
  install -m 755 "packages/opencode/dist/$TARGET/bin/opencode" "$STAGED_CLI"
fi

if [ "$SKIP_GUI" = 0 ]; then
  say "building GUI coordinator sidecar"
  bun run --cwd packages/opencode build --single --gui-coordinator --skip-install
  COORDINATOR="packages/opencode/dist/$TARGET/bin/opencode-gui-coordinator"
  # copy-sidecar.ts throws its own error for this, but only after the renderer
  # and main bundles have been rebuilt; failing here keeps the diagnosis next to
  # the build that was supposed to produce the binary.
  [ -x "$COORDINATOR" ] || die "coordinator build produced no executable at $COORDINATOR"

  say "packaging GUI app"
  # OPENCODEX_GUI_SIDECAR_VERSION: copy-sidecar.ts prefers this over
  # dist/<target>/package.json; both carry the same stamp, and setting it
  # explicitly means a stale dist cannot mis-stamp the bundle the version gate
  # reads.
  #
  # OPENCODEX_ALLOW_UNSIGNED_GUI: notarize.cjs hard-fails the package step
  # unless APPLE_ID / APPLE_APP_SPECIFIC_PASSWORD / APPLE_TEAM_ID are all set.
  # A local dogfood build never has them and its product is an unsigned local
  # app by definition, so this pipeline opts in on its own behalf instead of
  # requiring every caller to know the variable.  `:-` keeps it overridable —
  # an operator who exports 0 still gets the hard failure — and when the real
  # secrets ARE present notarize.cjs notarizes regardless of this value, so
  # defaulting it never downgrades a signed build.  Scoped to this command.
  OPENCODEX_GUI_SIDECAR_VERSION="$STAMP" \
  OPENCODEX_ALLOW_UNSIGNED_GUI="${OPENCODEX_ALLOW_UNSIGNED_GUI:-1}" \
    bun run --cwd packages/gui package

  BUILT_APP=""
  for candidate in packages/gui/release/mac-arm64/OpencodeX.app packages/gui/release/mac/OpencodeX.app \
                   packages/gui/release/mac-arm64/opencodex-gui.app packages/gui/release/mac/opencodex-gui.app; do
    [ -d "$candidate" ] && { BUILT_APP="$candidate"; break; }
  done
  if [ -z "$BUILT_APP" ]; then
    # electron-builder names the bundle from productName, which has changed
    # before (OpencodeX.app -> opencodex-gui.app).  When it produced exactly one
    # bundle there is nothing to disambiguate, so take it rather than fail a
    # deploy over a rename; more than one is genuinely ambiguous.
    found=""
    count=0
    for a in packages/gui/release/mac-arm64/*.app packages/gui/release/mac/*.app; do
      [ -d "$a" ] || continue
      found="$a"
      count=$((count + 1))
    done
    if [ "$count" -eq 1 ]; then
      BUILT_APP="$found"
      say "electron-builder named the bundle ${BUILT_APP##*/}; using it"
    elif [ "$count" -gt 1 ]; then
      die "electron-builder produced $count .app bundles under packages/gui/release; cannot tell which to deploy"
    fi
  fi
  [ -n "$BUILT_APP" ] || die "electron-builder produced no .app bundle under packages/gui/release"
  ditto "$BUILT_APP" "$STAGED_APP"
fi

# --------------------------------------------------------- lockstep gate ---
# Nothing installed has been touched yet.  If the artifacts disagree, stop here.
step "verify artifacts"
if [ "$GUI_ONLY" = 0 ]; then
  CLI_STAMP="$("$STAGED_CLI" --version 2>/dev/null || true)"
  say "CLI  $CLI_STAMP"
  [ "$CLI_STAMP" = "$STAMP" ] || die "LOCKSTEP FAIL  CLI stamped '$CLI_STAMP', expected '$STAMP'"
fi

if [ "$SKIP_GUI" = 0 ]; then
  PACKAGED="$STAGED_APP/Contents/Resources/sidecar/opencode-gui-coordinator"
  APP_STAMP="$(jq -r '.version // empty' "$STAGED_APP/Contents/Resources/sidecar/version.json" 2>/dev/null || true)"
  say "GUI  $APP_STAMP"
  [ "$APP_STAMP" = "$STAMP" ] || die "LOCKSTEP FAIL  GUI sidecar stamped '$APP_STAMP', expected '$STAMP'"
  # That stamp is the whole client half of the attach handshake:
  # `sidecarVersion()` in packages/gui/src/main/sidecar-launch.ts reads
  # resources/sidecar/version.json and nothing else, and the coordinator binary
  # is never asked its own version — it has no --version flag, it prints usage
  # for any unrecognised argument.  So version.json == $STAMP is the complete,
  # correct invariant, and what remains is that the bundle actually carries a
  # runnable coordinator behind the stamp it advertises.
  #
  # This deliberately does NOT sha256-compare against dist/.  That check can
  # never pass on macOS: electron-builder re-signs every Mach-O it bundles, so
  # dist/ is `adhoc,linker-signed` (Identifier=a.out) while the packaged copy
  # carries a hardened-runtime signature (Identifier=opencode-gui-coordinator)
  # and a different byte length.  It failed 100% of runs with the artifacts in
  # genuine lockstep, and it was over-specifying an identity the handshake does
  # not consult.  The freshness it was reaching for is already guaranteed
  # upstream: OPENCODEX_GUI_SIDECAR{,_TARGET} are unset above, the coordinator
  # is built immediately before packaging, and copy-sidecar.ts copies that dist
  # binary and writes version.json from the same build.
  [ -x "$PACKAGED" ] || die "LOCKSTEP FAIL  packaged app has no executable sidecar coordinator at $PACKAGED"
fi

CLI_HASH=""
[ "$GUI_ONLY" = 1 ] || CLI_HASH="$(shasum -a 256 "$STAGED_CLI" | cut -d ' ' -f 1)"
say "artifacts agree on $STAMP"

if [ "$STAGE_ONLY" = 1 ]; then
  step "stage only"
  [ "$GUI_ONLY" = 1 ] || say "  CLI  $STAGED_CLI  (sha256 $CLI_HASH)"
  [ "$SKIP_GUI" = 1 ] || say "  GUI  $STAGED_APP"
  say ""
  say "LOCKSTEP PASS (staged)  $STAMP  — nothing installed; re-run without --stage-only to deploy"
  exit 0
fi

# ------------------------------------------------------------------- drain --
# --gui-only never restarts the server, so in-flight turns are never at risk.
[ "$GUI_ONLY" = 1 ] || wait_for_drain

# ----------------------------------------------------------------- install --
restore_gui() {
  [ -n "$GUI_BACKUP" ] && [ -d "$GUI_BACKUP" ] || return 0
  say "restoring the previous GUI bundle"
  rm -rf "$GUI_INSTALL"
  ditto "$GUI_BACKUP" "$GUI_INSTALL"
}

if [ "$SKIP_GUI" = 0 ]; then
  step "install GUI"
  mkdir -p "$BACKUP_DIR"
  if [ -d "$GUI_INSTALL" ]; then
    GUI_BACKUP="$BACKUP_DIR/OpencodeX.app"
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
  ROLLBACK_ARMED=1
  # Move the old bundle aside before moving the new one in, so no failure can
  # leave /Applications without an OpencodeX.app at all.
  rm -rf "$GUI_INSTALL.oxd-incoming" "$GUI_INSTALL.oxd-old"
  ditto "$STAGED_APP" "$GUI_INSTALL.oxd-incoming"
  [ ! -d "$GUI_INSTALL" ] || mv "$GUI_INSTALL" "$GUI_INSTALL.oxd-old"
  mv "$GUI_INSTALL.oxd-incoming" "$GUI_INSTALL"
  rm -rf "$GUI_INSTALL.oxd-old"
  say "installed  $GUI_INSTALL"
fi

if [ "$GUI_ONLY" = 0 ]; then
  step "install CLI and restart server"
  say "handing off to $(basename "$CUTOVER") (nonce $CUTOVER_NONCE)"
  ROLLBACK_ARMED=1
  if ! CUTOVER_NONCE="$CUTOVER_NONCE" /bin/bash "$CUTOVER" "$STAGED_CLI" "$CLI_HASH" "$STAMP"; then
    # The cutover recovers the server to the old binary on its own, so leaving
    # the new GUI installed would strand it against a version the server no
    # longer runs — the exact-match attach gate would refuse every connection.
    restore_gui
    die "cutover failed; server and GUI were both returned to their previous versions"
  fi
fi

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

if [ "$OK" != 1 ]; then
  say ""
  say "LOCKSTEP FAIL  server='${FINAL_SERVER:-none}' gui='${FINAL_GUI:-none}' expected='$STAMP'"
  exit 1
fi

# ------------------------------------------------------------------ record --
mkdir -p "$STATE_DIR"
# last-deploy.json records what this script put on the server.  A GUI-only run
# put nothing there, so it must not claim the deploy that the cutover made.
if [ "$GUI_ONLY" = 0 ]; then
  jq -n --arg commit "$SHA" --arg stamp "$STAMP" --arg ref "$REF" --arg channel "$CHANNEL" \
    --arg cutover "$CUTOVER_ROOT" --arg guiBackup "$GUI_BACKUP" --arg at "$(date -u '+%FT%TZ')" \
    '{commit:$commit,stamp:$stamp,ref:$ref,channel:$channel,cutover:$cutover,guiBackup:$guiBackup,at:$at}' \
    > "$LAST.tmp" && mv "$LAST.tmp" "$LAST"
fi

# Keep the newest few rollback points and the one this deploy depends on; drop
# the rest, because nothing else on this machine ever reclaims them.
prune() {
  local dir="$1" pattern="$2" keep count entry
  [ -d "$dir" ] || return 0
  count=0
  for entry in $(ls -td "$dir"/$pattern 2>/dev/null); do
    count=$((count + 1))
    [ "$count" -gt "$KEEP_GENERATIONS" ] || continue
    case "$entry" in "$CUTOVER_ROOT" | "$BACKUP_DIR" | "$STAGE") continue ;; esac
    rm -rf "$entry"
  done
}
prune "$HOME/.opencode/cutovers" 'oxd-*'
prune "$STATE_DIR/backups" '*'
prune "$STATE_DIR/stage" '*'

print_rollback
say ""
say "LOCKSTEP PASS  $STAMP  $(lockstep_claim)  from $SHA"
[ "$SKIP_GUI" = 1 ] || say "relaunch OpencodeX.app to pick up the new GUI"
exit 0
