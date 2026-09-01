#!/usr/bin/env bash
# Fleet quickstart: the release tarball onto an already-provisioned fleet,
# a 3-node cluster + demo counter service up, one write proven end to end.
#
# This is docs/how-to/fleet_start.md steps 3-7 as one command. Provision first
# (steps 1-2), then:
#
#     cd bench-infra
#     make up-uc
#     scripts/fleet_quickstart.sh              # this script
#     make destroy                             # step 8 — ALWAYS, even on failure
#
# Flags:
#     --version V     release to install (default: 2.10.0)
#     --app-id A      application identity (default: myapp)
#
# Every step checks its outcome and fails loudly with the failing host and
# command; a green run ends with PASS. Re-runnable: a second run restarts the
# daemons with the same config and proves another write (the admin key and
# counter value survive).
#
# Nothing here tears the fleet down. `make destroy` is a separate, deliberate
# step, and it applies even when this script fails.
set -euo pipefail

VER="2.10.0"
APP="myapp"
while [ $# -gt 0 ]; do
    case "$1" in
        --version) VER="$2"; shift 2 ;;
        --app-id)  APP="$2"; shift 2 ;;
        *) echo "unknown flag: $1" >&2; exit 2 ;;
    esac
done

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

BENCH="$(cd "$(dirname "$0")/.." && pwd)"
TFVARS="$BENCH/terraform.tfvars"
SSH_KEY="$(awk -F'"' '/ssh_private_key_file/{print $2}' "$TFVARS")"
[ -f "$SSH_KEY" ] || fail "ssh key from terraform.tfvars not found: $SSH_KEY"
SSH_USER="$(terraform -chdir="$BENCH/terraform" output -raw ssh_user 2>/dev/null || echo ubuntu)"

# Hosts from terraform state — the same source `make status` prints.
mapfile -t PUB < <(terraform -chdir="$BENCH/terraform" output -json nodes | jq -r '.[].public_ip')
mapfile -t PRIV < <(terraform -chdir="$BENCH/terraform" output -json nodes | jq -r '.[].private_ip')
N=${#PUB[@]}
[ "$N" -eq 3 ] || fail "expected 3 hosts in terraform output, got $N — run 'make up-uc' first"
say "fleet: ${PUB[*]} (private: ${PRIV[*]})"

rssh() { # rssh <idx> <command...>
    local i="$1"; shift
    ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
        "$SSH_USER@${PUB[$i]}" "$@"
}

# ---------------------------------------------------------- 3. install release
say "3. installing release v$VER on every host"
for i in 0 1 2; do
    rssh "$i" "set -e
        ARCH=\$(uname -m)
        T=uc2-${VER}-\${ARCH}-unknown-linux-gnu
        cd /tmp
        curl -fsSLO \"https://github.com/PeterKnego/ultima_cluster/releases/download/v${VER}/\${T}.tar.gz\"
        curl -fsSLO \"https://github.com/PeterKnego/ultima_cluster/releases/download/v${VER}/\${T}.tar.gz.sha256\"
        sha256sum -c \"\${T}.tar.gz.sha256\"
        tar xzf \"\${T}.tar.gz\"
        sudo install -m 0755 \"\${T}\"/bin/* /usr/local/bin/
        sudo mkdir -p /opt/uc2-packaging
        sudo cp -r \"\${T}\"/packaging/. /opt/uc2-packaging/
        uc2-node --version && uc2ctl --version" \
        || fail "install on host $i (${PUB[$i]})"
done

# ---------------------------------------------------------- 4. one admin key
say "4. one admin key, shared by all hosts"
rssh 0 "sudo mkdir -p /etc/uc2 && sudo test -f /etc/uc2/admin.key || sudo uc2ctl gen-admin-key /etc/uc2/admin.key" \
    || fail "gen-admin-key on host 0"
KEY_TMP="$(mktemp)"
rssh 0 "sudo cat /etc/uc2/admin.key" > "$KEY_TMP" || fail "read admin.key from host 0"
for i in 1 2; do
    rssh "$i" "sudo mkdir -p /etc/uc2 && sudo tee /etc/uc2/admin.key >/dev/null && sudo chmod 0600 /etc/uc2/admin.key" \
        < "$KEY_TMP" || fail "copy admin.key to host $i"
done
rm -f "$KEY_TMP"
for i in 0 1 2; do rssh "$i" "sudo test -s /etc/uc2/admin.key" || fail "admin.key missing on host $i"; done

# ------------------------------------------------- 5. node.toml + uc2-node
say "5. writing node.toml + starting uc2-node on every host"
MEMBERS=""
for j in 0 1 2; do
    MEMBERS+="[[members]]
id = $j
addr = \"${PRIV[$j]}:9100\"

"
done
for i in 0 1 2; do
    rssh "$i" "set -e
        sudo mkdir -p /opt/bench/uc2/n$i && sudo ln -sfn /opt/bench/uc2 /srv/uc2
        sudo tee /etc/uc2/node.toml >/dev/null <<EOF
id = $i
bind = \"${PRIV[$i]}:9100\"
instance_dir = \"/srv/uc2/n$i\"
app_id = \"$APP\"

${MEMBERS}[crypto]
enabled = false

[admin]
auth = \"hmac\"
keys = [{ name = \"admin\", key_path = \"/etc/uc2/admin.key\" }]
EOF
        sudo install -m 0644 /opt/uc2-packaging/systemd/uc2-node.service /etc/systemd/system/
        sudo systemctl daemon-reload
        sudo systemctl enable --now uc2-node
        sudo systemctl restart uc2-node" \
        || fail "configure/start uc2-node on host $i"
done
for i in 0 1 2; do
    rssh "$i" "systemctl is-active --quiet uc2-node" \
        || { rssh "$i" "journalctl -u uc2-node -n 20 --no-pager" >&2 || true
             fail "uc2-node not active on host $i — refusal above"; }
done
say "   waiting for a serving leader"
LEADER=-1
for _ in $(seq 1 30); do
    for i in 0 1 2; do
        if rssh "$i" "sudo uc2ctl status --instance-dir /srv/uc2/n$i --app-id $APP 2>/dev/null" \
            | grep -q "leader=true can_serve=true"; then LEADER=$i; break 2; fi
    done
    sleep 1
done
[ "$LEADER" -ge 0 ] || fail "no serving leader within 30 s"
say "   leader: host $LEADER"

# ------------------------------------------------- 6. counter-service per host
say "6. starting the demo counter service on every host"
for i in 0 1 2; do
    rssh "$i" "set -e
        sudo sed 's|/srv/uc2/n0|/srv/uc2/n$i|; s|--app-id myapp|--app-id $APP|' \
            /opt/uc2-packaging/systemd/uc2-service@.service \
            | sudo tee /etc/systemd/system/uc2-service@.service >/dev/null
        sudo systemctl daemon-reload
        sudo systemctl enable --now uc2-service@counter-service
        sudo systemctl restart uc2-service@counter-service" \
        || fail "start counter-service on host $i"
done
sleep 2
for i in 0 1 2; do
    rssh "$i" "systemctl is-active --quiet uc2-service@counter-service" \
        || { rssh "$i" "journalctl -u uc2-service@counter-service -n 20 --no-pager" >&2 || true
             fail "counter-service not active on host $i"; }
done

# ------------------------------------------------- 7. end-to-end write proof
# One gateway PER host, each listing all three in [[members]] — a gateway on a
# follower REDIRECTS the client to the leader's gateway, so a single gateway
# only works while its own node happens to lead (run 3 of the validation
# proved that the hard way: leader moved to host 1, the lone host-0 gateway
# had nowhere to send the write, and the request timed out).
say "7. a gateway on every host + one proven write"
GW_MEMBERS=""
for j in 0 1 2; do
    GW_MEMBERS+="[[members]]
node_id = $j
gateway = \"${PRIV[$j]}:9200\"

"
done
for i in 0 1 2; do
    rssh "$i" "set -e
    sudo tee /etc/uc2/gw.toml >/dev/null <<EOF
[local]
instance_dir = \"/srv/uc2/n$i\"
app_id = \"$APP\"
listen = \"${PRIV[$i]}:9200\"

${GW_MEMBERS}[session]
envelope = false
EOF
    # Stop by UNIT, never 'pkill -f <pattern>': the pattern would match this
    # ssh session's own shell (its argv contains the systemd-run line below)
    # and kill it — which is exactly what the first validation run tripped on.
    sudo systemctl stop uc2-gw 2>/dev/null || true
    sudo systemctl reset-failed uc2-gw 2>/dev/null || true
    sudo systemd-run --unit=uc2-gw --collect uc2-gateway --config /etc/uc2/gw.toml" \
        || fail "start gateway on host $i"
done
GATEWAYS="${PRIV[0]}:9200,${PRIV[1]}:9200,${PRIV[2]}:9200"
sleep 1
# `get` prints exactly `value=<n>`; --linearizable routes it through the
# cluster's read barrier so the assertion below reflects every acked write.
BEFORE=$(rssh 0 "counter-remote --gateways $GATEWAYS --app-id $APP get --linearizable" | sed -n 's/^value=//p') \
    || fail "get through the gateway"
[ -n "$BEFORE" ] || fail "get printed no value= line"
rssh 0 "counter-remote --gateways $GATEWAYS --app-id $APP add 5" >/dev/null \
    || fail "add through the gateway"
AFTER=$(rssh 0 "counter-remote --gateways $GATEWAYS --app-id $APP get --linearizable" | sed -n 's/^value=//p') \
    || fail "get-back through the gateway"
[ "$AFTER" = "$((BEFORE + 5))" ] || fail "counter read $AFTER, expected $((BEFORE + 5))"
say "   write proven: counter $BEFORE -> $AFTER (client -> gateway -> quorum commit -> apply)"

say "PASS — 3-node cluster serving on ${PUB[0]} ${PUB[1]} ${PUB[2]} (leader: host $LEADER)"
echo "Remember: nothing auto-reaps this fleet. 'make destroy' when done (fleet_start.md step 8)."
