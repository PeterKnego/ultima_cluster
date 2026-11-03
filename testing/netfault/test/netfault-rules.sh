#!/usr/bin/env bash
# Checks every kind of rule against hand-made traffic between two throwaway containers, with no cluster: which rule
# each packet goes to, what the rules count, and what the receiver gets. Needs a ROOTFUL docker and nothing else
# (under rootless docker nothing can load BPF; on a box whose default context is rootless, DOCKER_CONTEXT=default).
#
# The sender carries one rule of each kind, each on a port of its own, and sends UC-shaped datagrams to addresses
# nobody answers (the egress hook sees them on the way out) and plain timestamped lines to the receiver. Delay,
# duplication and jitter are judged on arrival as well as by count; duplication of what arrives is judged on a rule
# attached to the receiver. Both containers read one kernel's /proc/uptime, so a latency is a difference of two.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT}/logs/netfault-rules"
SENDER=netfault-sender
RECEIVER=netfault-receiver
FAILURES=0

rm -rf "${LOG_DIR}"
mkdir -p "${LOG_DIR}"

cleanup() {
    docker rm -f "${SENDER}" "${RECEIVER}" > /dev/null 2>&1
}
trap cleanup EXIT

# fault <container> <command>... — the CLI in that container's network namespace.
fault() {
    docker run --rm --privileged --network "container:$1" netfault:local netfault "${@:2}"
}

pass() { echo "ok   $*"; }
fail() {
    echo "FAIL $*"
    FAILURES=$((FAILURES + 1))
}

# expect <stats file> <rule> <counter> <min> [max] — the rule is its stats line up to its settings, as in "9101 nak".
expect() {
    local n
    n="$(awk -v rule="$2" -v name="$3" '
        substr($0, 1, index($0, " loss=") - 1) == rule {
            for (f = 1; f <= NF; f++) { split($f, kv, "="); if (kv[1] == name) print kv[2] }
        }' "$1")"
    if [[ -n "${n}" ]] && ((n >= $4 && n <= ${5:-$4})); then
        pass "${2}: ${3}=${n}"
    else
        fail "${2}: ${3}=${n:-no such rule}, expected $4${5:+..$5}"
    fi
}

# Sent ahead of each batch of sender commands: dgram writes one datagram to the descriptor it is redirected to.
FRAME_LIB="$(
    cat << 'EOF'
le16() { printf '\\x%02x\\x%02x' $(($1 & 255)) $(($1 >> 8 & 255)); }
le32() { printf '%s%s' "$(le16 $(($1 & 65535)))" "$(le16 $(($1 >> 16 & 65535)))"; }
le64() { printf '%s%s' "$(le32 $(($1 & 4294967295)))" "$(le32 $(($1 >> 32)))"; }
# dgram <position> <term> <kind> <key_epoch> <payload bytes> — UC's 16-byte header (uc_protocol::v2::datagram:
# position u64, leadership_term_id u32, kind u8, wire u8, key_epoch u16, little-endian) and a zero payload.
dgram() {
    local d
    d="$(le64 "$1")$(le32 "$2")$(printf '\\x%02x' "$3")\\x0c$(le16 "$4")"
    ((${5} > 0)) && d+="$(printf '\\x00%.0s' $(seq 1 "$5"))"
    printf '%b' "${d}"
}
EOF
)"

# send <commands> — runs them in the sender, after FRAME_LIB, with RX set to the receiver's address.
send() {
    docker exec -i -e RX="${RX}" "${SENDER}" bash -s <<< "${FRAME_LIB}"$'\n'"$1"
}

# ── containers ─────────────────────────────────────────────────────────────────────────────────────
if ! docker build -q -t netfault:local "${ROOT}" > "${LOG_DIR}/build.log" 2>&1; then
    echo "netfault image did not build; see ${LOG_DIR}/build.log"
    exit 1
fi
cleanup
docker run -d --name "${SENDER}" debian:bookworm-slim sleep 3600 > /dev/null
# One listener a port, each line stamped on arrival. busybox nc keeps to its first peer, so each port gets one socket.
docker run -d --name "${RECEIVER}" alpine sh -c '
    for p in 9130 9131 9132 9133 9134; do
        (nc -u -l -p $p | while read i t; do echo "$i $t $(cut -d" " -f1 /proc/uptime)"; done > /tmp/rx-$p) &
    done
    sleep 3600' > /dev/null
RX="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${RECEIVER}")"

# ── rules ──────────────────────────────────────────────────────────────────────────────────────────
{
    fault "${SENDER}" attach
    # Kind, loss, and a kind before the port's any rule.
    fault "${SENDER}" block --kind nak 9101
    fault "${SENDER}" rule --kind data --loss 50 9102
    fault "${SENDER}" block 9103
    fault "${SENDER}" block --kind status 9103
    # Retransmission, per flow, per term, and under wire crypto.
    fault "${SENDER}" block --kind retransmit 9110 9111 9112
    fault "${SENDER}" rule --kind data --delay 1 9110 9111 9112
    # A peer before a port, and a peer on any port.
    fault "${SENDER}" rule --delay 1 9120
    fault "${SENDER}" block --peer 10.99.0.2 9120
    fault "${SENDER}" rule --peer 10.99.0.2 --delay 2
    # Delay, duplication and jitter, judged on arrival.
    fault "${SENDER}" rule --delay 200 9130
    fault "${SENDER}" rule --duplicate 100 9131
    fault "${SENDER}" rule --jitter 300 9132
    # Direction: an --out rule leaves what comes back alone, an --in rule what goes out.
    fault "${SENDER}" block --out --kind data 9140
    fault "${SENDER}" block --in --kind data 9141
    fault "${RECEIVER}" attach
    fault "${RECEIVER}" rule --duplicate 100 9133
    fault "${RECEIVER}" rule --out --duplicate 100 9134     # the receiver's replies, of which there are none
} > "${LOG_DIR}/rules.log" 2>&1 || {
    echo "could not install the rules; see ${LOG_DIR}/rules.log"
    exit 1
}

# ── traffic ────────────────────────────────────────────────────────────────────────────────────────
send '
exec 3> /dev/udp/10.99.0.1/9101 4> /dev/udp/10.99.0.1/9102 5> /dev/udp/10.99.0.1/9103
for i in {1..5}; do dgram 0 1 3 0 8 >&3; dgram 0 1 1 0 32 >&3; echo x >&3; done   # nak, data, not UC
for i in {1..400}; do dgram 0 1 1 0 32 >&4; done
for i in {1..3}; do dgram 0 1 4 0 8 >&5; dgram 0 1 1 0 32 >&5; done               # status, data

exec 3> /dev/udp/10.99.0.1/9110 4> /dev/udp/10.99.0.1/9111 5> /dev/udp/10.99.0.1/9112
dgram 0 7 1 0 64 >&3        # one flow in term 7: positions 0, 64, 0 again, 128
dgram 64 7 1 0 64 >&3
dgram 0 7 1 0 64 >&3        #   retransmitted
dgram 128 7 1 0 64 >&3
dgram 192 7 2 0 0 >&3       #   a heartbeat is not DATA and moves nothing
dgram 32 8 1 0 64 >&3       #   term 8 starts a new frontier: 32 is below 192 but not a retransmission
dgram 32 8 1 0 64 >&3       #   retransmitted in term 8
dgram 0 7 1 0 64 >&3        #   a stale term is neither a repair nor a frontier
dgram 0 7 1 0 64 >&4        # the same flow to another port: new, then a retransmission
dgram 0 7 1 0 64 >&4
dgram 0 7 1 1 64 >&5        # sealed (key_epoch 1): 64 wire bytes carry 40 of log, so the frontier is 40
dgram 40 7 1 1 64 >&5       #   the next datagram starts at 40: not a retransmission
dgram 39 7 1 1 64 >&5       #   one byte below it is

exec 3> /dev/udp/10.99.0.1/9120 4> /dev/udp/10.99.0.2/9120 5> /dev/udp/10.99.0.2/9121
dgram 0 1 1 0 32 >&3        # no peer rule: the port rule
dgram 0 1 1 0 32 >&4        # the peer rule on its port
dgram 0 1 1 0 32 >&5        # the peer rule on any port

exec 3> /dev/udp/10.99.0.1/9140 4> /dev/udp/10.99.0.1/9141
for i in {1..5}; do dgram 0 1 1 0 32 >&3; dgram 0 1 1 0 32 >&4; done   # both only ever leave the sender

exec 3> /dev/udp/$RX/9130 4> /dev/udp/$RX/9131 5> /dev/udp/$RX/9132 6> /dev/udp/$RX/9133 7> /dev/udp/$RX/9134
for i in {1..10}; do echo "$i $(cut -d" " -f1 /proc/uptime)" >&3; echo "$i x" >&4; echo "$i x" >&6; echo "$i x" >&7; sleep 0.05; done
for i in {1..20}; do echo "$i x" >&5; sleep 0.01; done
' 2> "${LOG_DIR}/send.log" || {
    echo "the sender failed; see ${LOG_DIR}/send.log"
    exit 1
}
sleep 1
fault "${SENDER}" stats > "${LOG_DIR}/sender.txt"
fault "${RECEIVER}" stats > "${LOG_DIR}/receiver.txt"
for p in 9130 9131 9132 9133 9134; do docker exec "${RECEIVER}" cat "/tmp/rx-${p}" > "${LOG_DIR}/rx-${p}.txt"; done

# ── checks ─────────────────────────────────────────────────────────────────────────────────────────
S="${LOG_DIR}/sender.txt"
echo "-- kinds and loss"
expect "${S}" "9101 nak" dropped 5
expect "${S}" "9102 data" dropped 150 250
expect "${S}" "9103 status" dropped 3
expect "${S}" "9103 any" dropped 3

echo "-- retransmission"
expect "${S}" "9110 retransmit" dropped 2
expect "${S}" "9110 data" delayed 5
expect "${S}" "9111 data" delayed 1
expect "${S}" "9111 retransmit" dropped 1
expect "${S}" "9112 data" delayed 2
expect "${S}" "9112 retransmit" dropped 1

echo "-- direction"
expect "${S}" "9140 data out" dropped 5
expect "${S}" "9141 data in" dropped 0
expect "${LOG_DIR}/receiver.txt" "9134 any out" duplicated 0

echo "-- peers"
expect "${S}" "9120 any" delayed 1
expect "${S}" "9120 any peer=10.99.0.2" dropped 1
expect "${S}" "any any peer=10.99.0.2" delayed 1

echo "-- delay, duplication and jitter on arrival"
expect "${S}" "9130 any" delayed 10
if awk 'END { exit NR != 10 } { if ($3 - $2 < 0.19 || $3 - $2 > 0.35) exit 1 }' "${LOG_DIR}/rx-9130.txt"; then
    pass "--delay 200: 10 arrived, each 0.19..0.35s late"
else
    fail "--delay 200: $(awk '{ printf "%.2f ", $3 - $2 } END { printf "(%d arrived)", NR }' "${LOG_DIR}/rx-9130.txt")"
fi
expect "${S}" "9131 any" duplicated 10
n="$(wc -l < "${LOG_DIR}/rx-9131.txt" | tr -d ' ')"
if ((n == 20)); then
    pass "--duplicate 100 going out: 20 of 10 arrived"
else
    fail "--duplicate 100 going out: ${n} of 10 arrived"
fi
expect "${LOG_DIR}/receiver.txt" "9133 any" duplicated 10
n="$(wc -l < "${LOG_DIR}/rx-9133.txt" | tr -d ' ')"
if ((n == 20)); then
    pass "--duplicate 100 coming in: 20 of 10 arrived"
else
    fail "--duplicate 100 coming in: ${n} of 10 arrived"
fi
expect "${S}" "9132 any" delayed 20
order="$(cut -d' ' -f1 "${LOG_DIR}/rx-9132.txt" | tr '\n' ' ')"
if [[ "$(wc -l < "${LOG_DIR}/rx-9132.txt" | tr -d ' ')" == 20 ]] \
    && ! cut -d' ' -f1 "${LOG_DIR}/rx-9132.txt" | sort -c -n 2> /dev/null; then
    pass "--jitter 300: 20 arrived reordered: ${order}"
else
    fail "--jitter 300: arrived as ${order}"
fi

echo "-- the CLI refuses what it cannot do"
for args in "block --kind bogus 1" "block --kind 65534 1" "block --type data 1" "block --out --in 1" "rule 1" "rule --loss 0 1" \
    "rule --delay 9000 --jitter 2000 1" "block --loss 5 1" "block 0" "block 65536" "block 9199 0" "remove x" \
    "block --peer no-such-host 1" "stats 1" "bogus"; do
    # shellcheck disable=SC2086
    if fault "${SENDER}" ${args} > /dev/null 2>&1; then fail "accepted: ${args}"; else pass "refused: ${args}"; fi
done

echo "-- remove and detach"
fault "${SENDER}" remove --kind status 9103
fault "${SENDER}" stats > "${LOG_DIR}/after-remove.txt"
if grep -q '^9199 ' "${LOG_DIR}/after-remove.txt"; then
    fail "block 9199 0 installed 9199 before refusing 0"
else
    pass "block 9199 0 installed nothing"
fi
if ! grep -q '^9103 status ' "${LOG_DIR}/after-remove.txt" && grep -q '^9103 any ' "${LOG_DIR}/after-remove.txt"; then
    pass "remove --kind status 9103 removed that rule and no other"
else
    fail "remove --kind status 9103: $(grep '^9103 ' "${LOG_DIR}/after-remove.txt" | cut -d' ' -f1-2 | tr '\n' ',')"
fi
fault "${SENDER}" detach
if fault "${SENDER}" stats > /dev/null 2>&1; then
    fail "stats after detach succeeded"
else
    pass "detach removed the rules"
fi

echo ""
if ((FAILURES == 0)); then
    echo "NETFAULT RULES TEST: PASS"
    exit 0
fi
echo "NETFAULT RULES TEST: FAIL (${FAILURES})"
exit 1
