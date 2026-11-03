#!/usr/bin/env bash
# Checks every kind of rule against hand-made traffic between two throwaway containers, with no cluster: which rule
# each packet goes to, what the rules count, and what the receiver gets. Needs docker and nothing else.
#
# The sender carries one rule of each kind, each on a port of its own, and sends Aeron-shaped datagrams to addresses
# nobody answers (the egress hook sees them on the way out) and plain timestamped lines to the receiver. Delay,
# duplication and jitter are judged on arrival as well as by count; duplication of what arrives is judged on a rule
# attached to the receiver. Both containers read one kernel's /proc/uptime, so a latency is a difference of two.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT}/logs/faulteron-rules"
SENDER=faulteron-sender
RECEIVER=faulteron-receiver
FAILURES=0

rm -rf "${LOG_DIR}"
mkdir -p "${LOG_DIR}"

cleanup() {
    docker rm -f "${SENDER}" "${RECEIVER}" > /dev/null 2>&1
}
trap cleanup EXIT

# fault <container> <command>... — the CLI in that container's network namespace.
fault() {
    docker run --rm --privileged --network "container:$1" faulteron:local faulteron "${@:2}"
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

# Sent ahead of each batch of sender commands: short and frame write one datagram to the descriptor they are
# redirected to.
FRAME_LIB="$(
    cat << 'EOF'
le16() { printf '\\x%02x\\x%02x' $(($1 & 255)) $(($1 >> 8 & 255)); }
le32() { printf '%s%s' "$(le16 $(($1 & 65535)))" "$(le16 $(($1 >> 16 & 65535)))"; }
# short <type> — the eight bytes every Aeron frame starts with, alone.
short() { printf '%b' "$(le32 8)\\x00\\x00$(le16 "$1")"; }
# frame <frame_length> <flags> <term_offset> <session> <template> <schema> — a 64-byte datagram holding one DATA
# frame on stream 1001, term 7, its payload an SBE header and zeros.
frame() {
    local f
    f="$(le32 "$1")\\x00$(printf '\\x%02x' "$2")$(le16 1)$(le32 "$3")$(le32 "$4")$(le32 1001)$(le32 7)"
    f+='\x00\x00\x00\x00\x00\x00\x00\x00'
    f+="$(le16 8)$(le16 "$5")$(le16 "$6")$(le16 1)"
    f+="$(printf '\\x00%.0s' {1..24})"
    printf '%b' "${f}"
}
EOF
)"

# send <commands> — runs them in the sender, after FRAME_LIB, with RX set to the receiver's address.
send() {
    docker exec -i -e RX="${RX}" "${SENDER}" bash -s <<< "${FRAME_LIB}"$'\n'"$1"
}

# ── containers ─────────────────────────────────────────────────────────────────────────────────────
if ! docker build -q -t faulteron:local "${ROOT}" > "${LOG_DIR}/build.log" 2>&1; then
    echo "faulteron image did not build; see ${LOG_DIR}/build.log"
    exit 1
fi
cleanup
docker run -d --name "${SENDER}" debian:bookworm-slim sleep 3600 > /dev/null
# One listener a port, each line stamped on arrival. busybox nc keeps to its first peer, so each port gets one socket.
docker run -d --name "${RECEIVER}" alpine sh -c '
    for p in 9130 9131 9132 9133; do
        (nc -u -l -p $p | while read i t; do echo "$i $t $(cut -d" " -f1 /proc/uptime)"; done > /tmp/rx-$p) &
    done
    sleep 3600' > /dev/null
RX="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${RECEIVER}")"

# ── rules ──────────────────────────────────────────────────────────────────────────────────────────
{
    fault "${SENDER}" attach
    # Frame type, loss, and a type before the port's any rule.
    fault "${SENDER}" block --type nak 9101
    fault "${SENDER}" rule --type data --loss 50 9102
    fault "${SENDER}" block 9103
    fault "${SENDER}" block --type sm 9103
    # Retransmission, per destination.
    fault "${SENDER}" block --type retransmit 9110 9111
    fault "${SENDER}" rule --type data --delay 1 9110 9111 9112 9113
    # Aeron Cluster templates, and templates and schemas of another schema.
    fault "${SENDER}" block --template request-vote 9112
    fault "${SENDER}" rule --template vote --delay 2 9112
    fault "${SENDER}" block --schema 101 --template 5 9113
    fault "${SENDER}" rule --schema 101 --delay 3 9113
    # A peer before a port, and a peer on any port.
    fault "${SENDER}" rule --delay 1 9120
    fault "${SENDER}" block --peer 10.99.0.2 9120
    fault "${SENDER}" rule --peer 10.99.0.2 --delay 2
    # Delay, duplication and jitter, judged on arrival.
    fault "${SENDER}" rule --delay 200 9130
    fault "${SENDER}" rule --duplicate 100 9131
    fault "${SENDER}" rule --jitter 300 9132
    fault "${RECEIVER}" attach
    fault "${RECEIVER}" rule --duplicate 100 9133
} > "${LOG_DIR}/rules.log" 2>&1 || {
    echo "could not install the rules; see ${LOG_DIR}/rules.log"
    exit 1
}

# ── traffic ────────────────────────────────────────────────────────────────────────────────────────
send '
exec 3> /dev/udp/10.99.0.1/9101 4> /dev/udp/10.99.0.1/9102 5> /dev/udp/10.99.0.1/9103
for i in {1..5}; do short 2 >&3; short 1 >&3; echo x >&3; done
for i in {1..400}; do short 1 >&4; done
for i in {1..3}; do short 3 >&5; short 1 >&5; done

exec 3> /dev/udp/10.99.0.1/9110 4> /dev/udp/10.99.0.1/9111 5> /dev/udp/10.99.0.1/9112 6> /dev/udp/10.99.0.1/9113
frame 64 192 0 1 1 111 >&3      # one flow: offsets 0, 64, 0 again, 128, then a heartbeat
frame 64 192 64 1 1 111 >&3
frame 64 192 0 1 1 111 >&3
frame 64 192 128 1 1 111 >&3
frame 0 192 192 1 1 111 >&3
frame 64 192 0 1 1 111 >&4      # the same flow to another port: new, then a retransmission
frame 64 192 0 1 1 111 >&4
frame 64 192 0 2 51 111 >&5     # request-vote
frame 64 192 64 2 52 111 >&5    # vote
frame 64 192 128 2 51 101 >&5   # its template id in another schema
frame 64 64 192 2 51 111 >&5    # request-vote, but not a first fragment
frame 64 192 0 2 51 111 >&5     # request-vote again, retransmitted, with no retransmit rule on the port
frame 64 192 0 3 5 101 >&6      # schema 101 template 5
frame 64 192 64 3 6 101 >&6     # schema 101 template 6
frame 64 192 128 3 5 111 >&6    # template 5 in schema 111

exec 3> /dev/udp/10.99.0.1/9120 4> /dev/udp/10.99.0.2/9120 5> /dev/udp/10.99.0.2/9121
frame 64 192 0 4 1 111 >&3      # no peer rule: the port rule
frame 64 192 0 5 1 111 >&4      # the peer rule on its port
frame 64 192 0 6 1 111 >&5      # the peer rule on any port

exec 3> /dev/udp/$RX/9130 4> /dev/udp/$RX/9131 5> /dev/udp/$RX/9132 6> /dev/udp/$RX/9133
for i in {1..10}; do echo "$i $(cut -d" " -f1 /proc/uptime)" >&3; echo "$i x" >&4; echo "$i x" >&6; sleep 0.05; done
for i in {1..20}; do echo "$i x" >&5; sleep 0.01; done
' 2> "${LOG_DIR}/send.log" || {
    echo "the sender failed; see ${LOG_DIR}/send.log"
    exit 1
}
sleep 1
fault "${SENDER}" stats > "${LOG_DIR}/sender.txt"
fault "${RECEIVER}" stats > "${LOG_DIR}/receiver.txt"
for p in 9130 9131 9132 9133; do docker exec "${RECEIVER}" cat "/tmp/rx-${p}" > "${LOG_DIR}/rx-${p}.txt"; done

# ── checks ─────────────────────────────────────────────────────────────────────────────────────────
S="${LOG_DIR}/sender.txt"
echo "-- frame types and loss"
expect "${S}" "9101 nak" dropped 5
expect "${S}" "9102 data" dropped 150 250
expect "${S}" "9103 sm" dropped 3
expect "${S}" "9103 any" dropped 3

echo "-- retransmission"
expect "${S}" "9110 retransmit" dropped 1
expect "${S}" "9110 data" delayed 4
expect "${S}" "9111 data" delayed 1
expect "${S}" "9111 retransmit" dropped 1

echo "-- schemas and templates"
expect "${S}" "9112 data schema=111 template=request-vote" dropped 2
expect "${S}" "9112 data schema=111 template=vote" delayed 1
expect "${S}" "9112 data" delayed 2
expect "${S}" "9113 data schema=101 template=5" dropped 1
expect "${S}" "9113 data schema=101" delayed 1
expect "${S}" "9113 data" delayed 1

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
for args in "block --type bogus 1" "rule 1" "rule --loss 0 1" "rule --delay 9000 --jitter 2000 1" "block --loss 5 1" \
    "block 0" "block 65536" "block 9199 0" "remove x" "block --schema 70000 1" "block --peer no-such-host 1" \
    "block --template 5 --type nak 1" "stats 1" "bogus"; do
    # shellcheck disable=SC2086
    if fault "${SENDER}" ${args} > /dev/null 2>&1; then fail "accepted: ${args}"; else pass "refused: ${args}"; fi
done

echo "-- remove and detach"
fault "${SENDER}" remove --type sm 9103
fault "${SENDER}" stats > "${LOG_DIR}/after-remove.txt"
if grep -q '^9199 ' "${LOG_DIR}/after-remove.txt"; then
    fail "block 9199 0 installed 9199 before refusing 0"
else
    pass "block 9199 0 installed nothing"
fi
if ! grep -q '^9103 sm ' "${LOG_DIR}/after-remove.txt" && grep -q '^9103 any ' "${LOG_DIR}/after-remove.txt"; then
    pass "remove --type sm 9103 removed that rule and no other"
else
    fail "remove --type sm 9103: $(grep '^9103 ' "${LOG_DIR}/after-remove.txt" | cut -d' ' -f1-2 | tr '\n' ',')"
fi
fault "${SENDER}" detach
if fault "${SENDER}" stats > /dev/null 2>&1; then
    fail "stats after detach succeeded"
else
    pass "detach removed the rules"
fi

echo ""
if ((FAILURES == 0)); then
    echo "FAULTERON RULES TEST: PASS"
    exit 0
fi
echo "FAULTERON RULES TEST: FAIL (${FAILURES})"
exit 1
