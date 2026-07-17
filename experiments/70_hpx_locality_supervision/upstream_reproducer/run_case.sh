#!/usr/bin/env bash
# exp70 upstream_reproducer driver (EXPERIMENT-ONLY, HPX-only; bash 3.2 compatible).
#
# Usage: ./run_case.sh <case>
#   case: late-dispatch-current-behavior | external-lifecycle-workaround
#
# Starts the exp70 root and connector on loopback in an isolated temp dir, captures stdout /
# stderr / PIDs / exit codes separately, enforces an overall wall-clock timeout, kills all
# children on failure or interruption, verifies no launched process remains alive, and prints
# ONE final machine-readable JSON summary line on stdout (all other logging goes to stderr).
# Exits 0 only when the selected case meets its expected result.
#
# Env overrides: BUILD_DIR TIMEOUT_S SERVE_WINDOW_S IDLE_DWELL_S DEADMAN_S DISPATCH_BOUND_S
#                HARD_TIMEOUT_S HPX_THREADS KEEP_WORKDIR

set -u

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
BUILD_DIR=${BUILD_DIR:-"$SCRIPT_DIR/build"}
TIMEOUT_S=${TIMEOUT_S:-120}
SERVE_WINDOW_S=${SERVE_WINDOW_S:-3}
IDLE_DWELL_S=${IDLE_DWELL_S:-6}
DEADMAN_S=${DEADMAN_S:-15}
DISPATCH_BOUND_S=${DISPATCH_BOUND_S:-10}
HARD_TIMEOUT_S=${HARD_TIMEOUT_S:-90}
HPX_THREADS=${HPX_THREADS:-2}
KEEP_WORKDIR=${KEEP_WORKDIR:-1}

log() { echo "run_case: $*" >&2; }

# ---- argument -------------------------------------------------------------------------------
if [ "$#" -ne 1 ]; then
    log "usage: $0 <late-dispatch-current-behavior|external-lifecycle-workaround>"
    exit 64
fi
CASE="$1"
case "$CASE" in
    late-dispatch-current-behavior|external-lifecycle-workaround) ;;
    *) log "unknown case: $CASE"; exit 64 ;;
esac

for bin in exp70_root exp70_connector; do
    if [ ! -x "$BUILD_DIR/$bin" ]; then
        log "missing binary $BUILD_DIR/$bin (build first: cmake -S . -B build -DHPX_DIR=...; cmake --build build)"
        exit 66
    fi
done

# ---- free loopback ports --------------------------------------------------------------------
# Random high port probed with nc when available. TOCTOU caveat: the port is only known-free at
# probe time; an hpx bind failure surfaces as a root/connector startup failure and a nonzero
# driver exit, so a rare collision fails loud, not silent.
pick_port() {
    p=0; tries=0
    while [ "$tries" -lt 50 ]; do
        p=$(( (RANDOM % 40000) + 20000 ))
        if command -v nc >/dev/null 2>&1; then
            if ! nc -z 127.0.0.1 "$p" >/dev/null 2>&1; then echo "$p"; return 0; fi
        else
            echo "$p"; return 0
        fi
        tries=$((tries + 1))
    done
    echo "$p"
}
ROOT_PORT=$(pick_port)
CONN_PORT=$(pick_port)
while [ "$CONN_PORT" = "$ROOT_PORT" ]; do CONN_PORT=$(pick_port); done

# ---- isolated workdir -------------------------------------------------------------------------
WORK=$(mktemp -d "${TMPDIR:-/tmp}/exp70_${CASE}.XXXXXX") || { log "mktemp failed"; exit 65; }
log "workdir: $WORK (root port $ROOT_PORT, connector port $CONN_PORT)"

ROOT_PID=""
CONN_PID=""

kill_children() {
    for pid in $ROOT_PID $CONN_PID; do
        [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && kill -TERM "$pid" 2>/dev/null
    done
    sleep 1
    for pid in $ROOT_PID $CONN_PID; do
        [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null
    done
}
trap 'kill_children' INT TERM

# ---- start root, then connector ----------------------------------------------------------------
"$BUILD_DIR/exp70_root" \
    --case="$CASE" --bootstrap="$WORK" \
    --dispatch-bound-s="$DISPATCH_BOUND_S" --idle-dwell-s="$IDLE_DWELL_S" \
    --hard-timeout-s="$HARD_TIMEOUT_S" \
    --hpx:hpx=127.0.0.1:"$ROOT_PORT" --hpx:agas=127.0.0.1:"$ROOT_PORT" \
    --hpx:expect-connecting-localities --hpx:threads="$HPX_THREADS" \
    >"$WORK/root.out" 2>"$WORK/root.err" &
ROOT_PID=$!

# Wait (bounded) for the root runtime to be up before launching the connector.
i=0
while [ "$i" -lt 200 ]; do
    grep -q '"started":true' "$WORK/root.started" 2>/dev/null && break
    if ! kill -0 "$ROOT_PID" 2>/dev/null; then break; fi
    sleep 0.1; i=$((i + 1))
done
if ! grep -q '"started":true' "$WORK/root.started" 2>/dev/null; then
    log "root failed to start (see $WORK/root.err)"
    kill_children
    wait "$ROOT_PID" 2>/dev/null
    echo "{\"case\":\"$CASE\",\"overall\":\"fail\",\"note\":\"root_failed_to_start\",\"workdir\":\"$WORK\"}"
    exit 1
fi

"$BUILD_DIR/exp70_connector" \
    --case="$CASE" --bootstrap="$WORK" \
    --serve-window-s="$SERVE_WINDOW_S" --deadman-s="$DEADMAN_S" \
    --hard-timeout-s="$HARD_TIMEOUT_S" \
    --hpx:hpx=127.0.0.1:"$CONN_PORT" --hpx:agas=127.0.0.1:"$ROOT_PORT" \
    --hpx:threads="$HPX_THREADS" \
    >"$WORK/connector.out" 2>"$WORK/connector.err" &
CONN_PID=$!

printf '{"root_pid":%s,"connector_pid":%s}\n' "$ROOT_PID" "$CONN_PID" >"$WORK/pids.json"

# ---- overall timeout ---------------------------------------------------------------------------
TIMED_OUT=false
deadline=$(( $(date +%s) + TIMEOUT_S ))
while :; do
    alive=0
    kill -0 "$ROOT_PID" 2>/dev/null && alive=1
    kill -0 "$CONN_PID" 2>/dev/null && alive=1
    [ "$alive" -eq 0 ] && break
    if [ "$(date +%s)" -ge "$deadline" ]; then
        TIMED_OUT=true
        log "overall timeout (${TIMEOUT_S}s) -- killing children"
        kill_children
        break
    fi
    sleep 0.2
done

wait "$ROOT_PID" 2>/dev/null; ROOT_RC=$?
wait "$CONN_PID" 2>/dev/null; CONN_RC=$?
echo "$ROOT_RC" >"$WORK/root.rc"
echo "$CONN_RC" >"$WORK/connector.rc"

# ---- no-orphan verification --------------------------------------------------------------------
ORPHANS=0
for pid in $ROOT_PID $CONN_PID; do
    kill -0 "$pid" 2>/dev/null && ORPHANS=$((ORPHANS + 1))
done
if command -v pgrep >/dev/null 2>&1; then
    extra=$(pgrep -f "$WORK" 2>/dev/null | wc -l | tr -d ' ')
    [ -n "$extra" ] && ORPHANS=$((ORPHANS + extra))
fi

# ---- marker extraction helpers ------------------------------------------------------------------
# Markers are one-line flat JSON written by the binaries. jget: bare value (bool/number);
# jgets: string value. Empty -> caller default.
jget()  { [ -f "$1" ] && sed -n 's/.*"'"$2"'":\([^,}"]*\).*/\1/p' "$1" | head -1; }
jgets() { [ -f "$1" ] && sed -n 's/.*"'"$2"'":"\([^"]*\)".*/\1/p' "$1" | head -1; }
bool_or_false() { v="$1"; [ "$v" = "true" ] && echo true || echo false; }

A1_OK=$(bool_or_false "$(jget "$WORK/root.action1" ok)")
A2_ATTEMPTED=false
[ -f "$WORK/root.action2_attempt" ] && A2_ATTEMPTED=true
A2_OK=$(bool_or_false "$(jget "$WORK/root.action2" ok)")
A2_AFTER_STOP=$(bool_or_false "$(jget "$WORK/root.action2" attempted_after_connector_stopped)")
STOPPING_SEEN=$(bool_or_false "$(jget "$WORK/root.observed_stop" stopping_seen)")
STOPPED_SEEN=$(bool_or_false "$(jget "$WORK/root.observed_stop" stopped_seen)")
NORMAL_COMPLETION=$(bool_or_false "$(jget "$WORK/connector.stopped" normal_completion)")
DEADMAN_EXPIRED=$(bool_or_false "$(jget "$WORK/connector.stopped" deadman_expired)")
SERVE_WINDOW_EXPIRED=$(bool_or_false "$(jget "$WORK/connector.stopped" serve_window_expired)")
HPX_VERSION=$(jgets "$WORK/root.started" hpx_version); HPX_VERSION=${HPX_VERSION:-unknown}
HPX_COMMIT=$(jgets "$WORK/root.started" hpx_git_commit); HPX_COMMIT=${HPX_COMMIT:-unknown}

# Exact second-dispatch outcome: the raw one-line JSON object recorded by the root.
A2_OUTCOME="null"
if [ -f "$WORK/root.action2" ]; then
    A2_OUTCOME=$(sed -n 's/.*"outcome":\({.*}\),"unix".*/\1/p' "$WORK/root.action2" | head -1)
    [ -z "$A2_OUTCOME" ] && A2_OUTCOME="null"
fi

# connector_stopped_before_second_dispatch: the case-1 ordering proof (root observed the
# connector.stopped marker before attempting dispatch 2). Always false in the workaround case.
STOP_BEFORE_A2=false
[ "$STOPPED_SEEN" = true ] && [ "$A2_AFTER_STOP" = true ] && STOP_BEFORE_A2=true

# race_constructed (case 1): action 1 verified, connector entered AND completed its stop path,
# and dispatch 2 was attempted strictly afterward.
RACE_CONSTRUCTED=false
if [ "$CASE" = "late-dispatch-current-behavior" ] && [ "$A1_OK" = true ] && \
   [ "$STOPPING_SEEN" = true ] && [ "$STOP_BEFORE_A2" = true ] && [ "$A2_ATTEMPTED" = true ]; then
    RACE_CONSTRUCTED=true
fi

# historical_exact_symptom_reproduced: STRICT -- true only when the captured root-side outcome
# is the exp63 signature (throw at the async call site, std::system_error, code 1).
HIST=false
if [ "$A2_OUTCOME" != "null" ]; then
    A2_STATUS=$(jgets "$WORK/root.action2" status)
    A2_ETYPE=$(jgets "$WORK/root.action2" error_type)
    A2_SITE=$(jgets "$WORK/root.action2" throw_site)
    A2_SYSCODE=$(jget "$WORK/root.action2" system_error_code)
    if [ "$A2_STATUS" = "threw" ] && [ "$A2_ETYPE" = "std::system_error" ] && \
       [ "$A2_SITE" = "async_call" ] && [ "$A2_SYSCODE" = "1" ]; then
        HIST=true
    fi
fi

# Late-parcel evidence (case-2 gate must be free of it): the exp63 connector-side signature in
# either process's captured stderr.
LATE_PARCEL=false
if grep -q "thread pool is not running\|load_schedule" \
        "$WORK/root.err" "$WORK/connector.err" 2>/dev/null; then
    LATE_PARCEL=true
fi

# root_silence_observed: the workaround deadman fired (root.alive silent past the deadman).
ROOT_SILENCE="$DEADMAN_EXPIRED"

# ---- per-case expected result --------------------------------------------------------------------
OVERALL=fail
if [ "$TIMED_OUT" = false ] && [ "$ORPHANS" -eq 0 ]; then
    if [ "$CASE" = "late-dispatch-current-behavior" ]; then
        # Expected: ordering constructed, dispatch 2 attempted and NOT verified-successful; the
        # exact failure mode is recorded, not prescribed (see README).
        if [ "$ROOT_RC" -eq 0 ] && [ "$CONN_RC" -eq 0 ] && [ "$A1_OK" = true ] && \
           [ "$RACE_CONSTRUCTED" = true ] && [ "$SERVE_WINDOW_EXPIRED" = true ] && \
           [ "$A2_ATTEMPTED" = true ] && [ "$A2_OK" = false ]; then
            OVERALL=pass
        fi
    else
        # Expected: both dispatches verified, connector left via the explicit completion
        # witness, deadman never fired, no late-parcel evidence, clean exits.
        if [ "$ROOT_RC" -eq 0 ] && [ "$CONN_RC" -eq 0 ] && [ "$A1_OK" = true ] && \
           [ "$A2_OK" = true ] && [ "$NORMAL_COMPLETION" = true ] && \
           [ "$ROOT_SILENCE" = false ] && [ "$LATE_PARCEL" = false ]; then
            OVERALL=pass
        fi
    fi
fi

SUMMARY="{\"case\":\"$CASE\",\"overall\":\"$OVERALL\",\"root_rc\":$ROOT_RC,\"connector_rc\":$CONN_RC,\"first_dispatch_succeeded\":$A1_OK,\"connector_stopped_before_second_dispatch\":$STOP_BEFORE_A2,\"second_dispatch_attempted\":$A2_ATTEMPTED,\"second_dispatch_succeeded\":$A2_OK,\"normal_completion_observed\":$NORMAL_COMPLETION,\"root_silence_observed\":$ROOT_SILENCE,\"race_constructed\":$RACE_CONSTRUCTED,\"historical_exact_symptom_reproduced\":$HIST,\"observed_second_dispatch_outcome\":$A2_OUTCOME,\"late_parcel_evidence_seen\":$LATE_PARCEL,\"timed_out_overall\":$TIMED_OUT,\"orphan_count\":$ORPHANS,\"hpx_version\":\"$HPX_VERSION\",\"hpx_git_commit\":\"$HPX_COMMIT\",\"workdir\":\"$WORK\"}"
echo "$SUMMARY" >"$WORK/summary.json"

if [ "$KEEP_WORKDIR" = "0" ] && [ "$OVERALL" = pass ]; then
    rm -rf "$WORK"
fi

echo "$SUMMARY"
[ "$OVERALL" = pass ]
