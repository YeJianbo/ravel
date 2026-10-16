#!/usr/bin/env bash
set -euo pipefail
umask 077

readonly repo=YeJianbo/ravel
readonly directory=/usr/local/ravel
readonly config_directory=/etc/ravel
version= panel= machine_id= migrate=0 token_stdin=0 enroll_stdin=0 bundle=
while (($#)); do
    case "$1" in
        --version) version=${2:?}; shift 2 ;;
        --panel) panel=${2:?}; shift 2 ;;
        --machine-id) machine_id=${2:?}; shift 2 ;;
        --bundle) bundle=${2:?}; shift 2 ;;
        --token-stdin) token_stdin=1; shift ;;
        --enroll-token-stdin) enroll_stdin=1; shift ;;
        --migrate) migrate=1; shift ;;
        --help) printf '%s\n' 'ravel-install.sh [--version vX.Y.Z] [--panel https://panel --machine-id ID] [--token-stdin | --enroll-token-stdin] [--migrate] [--bundle FILE]'; exit 0 ;;
        *) printf 'Unknown option: %s\n' "$1" >&2; exit 1 ;;
    esac
done
[[ $EUID == 0 ]] || { printf '%s\n' 'Run as root.' >&2; exit 1; }
for tool in curl jq unzip sha256sum install; do command -v "$tool" >/dev/null || { printf 'Missing dependency: %s\n' "$tool" >&2; exit 1; }; done
[[ -z "$version" || "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || exit 1
if [[ -n "$panel" || -n "$machine_id" ]]; then
    [[ "$panel" =~ ^https://[^/@?#[:space:]]+(/[^?#[:space:]]*)?$ ]] || { printf '%s\n' 'Require HTTPS panel.' >&2; exit 1; }
    [[ "$machine_id" =~ ^[1-9][0-9]*$ || ( $enroll_stdin == 1 && -z "$machine_id" ) ]] || { printf '%s\n' 'Require positive machine ID or enrollment.' >&2; exit 1; }
fi
[[ ! ( $enroll_stdin == 1 && $token_stdin == 1 ) ]] || exit 1
[[ $enroll_stdin == 0 || -n "$panel" ]] || exit 1
if [[ -d /run/openrc ]] && command -v rc-service >/dev/null; then manager=openrc
elif command -v systemctl >/dev/null; then manager=systemd
else printf '%s\n' 'Require systemd or OpenRC.' >&2; exit 1; fi
case "$(uname -m)" in
    x86_64|amd64) arch=64 ;;
    aarch64|arm64) arch=arm64-v8a ;;
    *) printf '%s\n' 'Installer supports Linux amd64 and arm64; other release binaries can be installed manually.' >&2; exit 1 ;;
esac
[[ "$(uname -s)" == Linux ]] || exit 1
if [[ -d /etc/.buncloud-agent || -d /usr/local/v2node ]]; then
    [[ $migrate == 1 || -f "$config_directory/state.json" ]] || { printf '%s\n' 'Existing installation detected. Review migration and use --migrate.' >&2; exit 1; }
fi
if [[ $migrate == 1 && -e "$config_directory" ]]; then
    printf '%s\n' 'Migration destination already exists; no files changed.' >&2; exit 1
fi
stage=$(mktemp -d /tmp/ravel-install.XXXXXXXX)
cleanup() { case "$stage" in /tmp/ravel-install.*) rm -rf -- "$stage" ;; esac; }
trap cleanup EXIT
if [[ -n "$bundle" ]]; then
    cp -- "$bundle" "$stage/release.zip"
    cp -- "$bundle.dgst" "$stage/release.dgst"
else
    if [[ -z "$version" ]]; then
        version=$(curl --fail --silent --show-error --max-time 30 "https://api.github.com/repos/$repo/releases/latest" | jq -er '.tag_name')
    fi
    [[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || exit 1
    url="https://github.com/$repo/releases/download/$version/ravel-linux-$arch.zip"
    curl --fail --location --proto '=https' --proto-redir '=https' --silent --show-error --max-time 180 "$url" -o "$stage/release.zip"
    curl --fail --location --proto '=https' --proto-redir '=https' --silent --show-error --max-time 30 "$url.dgst" -o "$stage/release.dgst"
fi
expected=$(awk '{name=toupper($1); gsub(/=/,"",name); if(name=="SHA256" || name=="SHA2-256") print tolower($NF)}' "$stage/release.dgst")
[[ "$expected" =~ ^[0-9a-f]{64}$ ]] || { printf '%s\n' 'Invalid SHA-256 digest.' >&2; exit 1; }
actual=$(sha256sum "$stage/release.zip"); actual=${actual%% *}
[[ "$actual" == "$expected" ]] || { printf '%s\n' 'Release digest mismatch.' >&2; exit 1; }
for entry in ravel ravel.sh install.sh geoip.dat geosite.dat LICENSE NOTICE; do
    unzip -p "$stage/release.zip" "$entry" > "$stage/$entry"
    [[ -s "$stage/$entry" ]] || exit 1
done
chmod 700 "$stage/ravel"
identity=$("$stage/ravel" version)
[[ "$identity" == 'ravel '* ]] || { printf '%s\n' 'Unexpected binary identity.' >&2; exit 1; }
if [[ -n "$version" ]]; then [[ "$identity" == "ravel $version "* ]] || { printf '%s\n' 'Binary version mismatch.' >&2; exit 1; }; fi
mkdir "$stage/config"
if [[ $migrate == 1 ]]; then
    [[ -f /etc/.buncloud-agent/state.json ]] || { printf '%s\n' 'No managed runtime state to migrate.' >&2; exit 1; }
    cp -a /etc/.buncloud-agent/. "$stage/config/"
    if [[ -d /etc/v2node ]]; then
        for certificate in /etc/v2node/*.cer /etc/v2node/*.crt; do
            [[ -f "$certificate" ]] || continue
            cp -a "$certificate" "$stage/config/"
            key=${certificate%.*}.key
            [[ ! -f "$key" ]] || cp -a "$key" "$stage/config/"
        done
    fi
elif [[ -d "$config_directory" ]]; then
    cp -a "$config_directory/." "$stage/config/"
fi
if [[ $enroll_stdin == 1 ]]; then
    IFS= read -r token || [[ -n ${token:-} ]]
    token=${token%$'\r'}
    [[ ${#token} -ge 16 && ${#token} -le 512 ]] || { printf '%s\n' 'Invalid enrollment token.' >&2; exit 1; }
    printf '%s' "$token" > "$stage/enroll-token"
    unset token
    jq -n --arg name "$(hostname)" --arg id "$machine_id" --rawfile token "$stage/enroll-token" '{enroll_token:$token,name:$name} + (if $id=="" then {} else {machine_id:($id|tonumber)} end)' > "$stage/enroll-request.json"
    curl --fail --silent --show-error --proto '=https' --max-time 30 -H 'Content-Type: application/json' --data-binary "@$stage/enroll-request.json" "${panel%/}/api/v1/server/machine/enroll" > "$stage/enroll-response.json"
    jq -e '.data.machine_id>0 and (.data.api_token|type=="string" and length>=16)' "$stage/enroll-response.json" >/dev/null
    jq --arg panel "${panel%/}" '{panel_url:$panel,machine_id:.data.machine_id,machine_token:.data.api_token,protocol:"ravel-v1",sync_interval:30,status_interval:120}' "$stage/enroll-response.json" > "$stage/config/state.json"
elif [[ -n "$panel" ]]; then
    if [[ $token_stdin == 1 ]]; then IFS= read -r token || [[ -n ${token:-} ]]
    else IFS= read -r -s -p 'Machine token: ' token; printf '\n'; fi
    token=${token%$'\r'}
    [[ ${#token} -ge 16 && ${#token} -le 512 ]] || { printf '%s\n' 'Invalid machine token.' >&2; exit 1; }
    printf '%s' "$token" > "$stage/token"
    unset token
    jq -n --arg panel "${panel%/}" --argjson id "$machine_id" --rawfile token "$stage/token" '{panel_url:$panel,machine_id:$id,machine_token:$token,protocol:"ravel-v1",sync_interval:30,status_interval:120}' > "$stage/config/state.json"
fi
jq -e '.protocol == "ravel-v1" and (.machine_id|type=="number") and .machine_id>0 and (.machine_token|type=="string" and length>=16) and (.panel_url|startswith("https://"))' "$stage/config/state.json" >/dev/null
if [[ ! -f "$stage/config/config.enc.json" ]]; then
    "$stage/ravel" config keygen > "$stage/config/config.key"
    if [[ ! -f "$stage/config/config.json" ]]; then
        printf '%s' '{"Log":{"Level":"warning","Access":"none"},"Nodes":[]}' > "$stage/config/config.json"
    fi
    RAVEL_CONFIG_KEY_FILE="$stage/config/config.key" "$stage/ravel" config encrypt --in "$stage/config/config.json" --out "$stage/config/config.enc.json"
    rm -f "$stage/config/config.json"
fi
RAVEL_CONFIG_KEY_FILE="$stage/config/config.key" "$stage/ravel" config decrypt --in "$stage/config/config.enc.json" --out "$stage/check.json"
jq -e '.Nodes|type=="array"' "$stage/check.json" >/dev/null
mkdir -p /var/backups
backup=$(mktemp -d /var/backups/ravel-install.XXXXXXXX)
for source in "$directory/ravel" "$config_directory" /usr/local/bin/ravel /etc/.buncloud-agent /etc/systemd/system/ravel.service /etc/init.d/ravel; do
    [[ ! -e "$source" ]] || cp -a --parents "$source" "$backup/"
done
old_service=
if [[ $manager == openrc ]]; then
    if rc-service ravel status >/dev/null 2>&1; then old_service=ravel
    elif [[ $migrate == 1 ]] && rc-service v2node status >/dev/null 2>&1; then old_service=v2node; fi
else
    if systemctl is-active --quiet ravel.service; then old_service=ravel
    elif [[ $migrate == 1 ]] && systemctl is-active --quiet v2node.service; then old_service=v2node; fi
fi
rollback() {
    printf 'Start failed. Restoring previous runtime; backup: %s\n' "$backup" >&2
    if [[ $manager == openrc ]]; then rc-service ravel stop >/dev/null 2>&1 || true
    else systemctl stop ravel.service >/dev/null 2>&1 || true; fi
    [[ ! -f "$backup$directory/ravel" ]] || install -m 755 "$backup$directory/ravel" "$directory/ravel"
    [[ ! -d "$backup$config_directory" ]] || cp -a "$backup$config_directory/." "$config_directory/"
    [[ ! -f "$backup/usr/local/bin/ravel" ]] || install -m 755 "$backup/usr/local/bin/ravel" /usr/local/bin/ravel
    if [[ $manager == openrc ]]; then
        [[ ! -f "$backup/etc/init.d/ravel" ]] || cp -a "$backup/etc/init.d/ravel" /etc/init.d/ravel
        [[ -z "$old_service" ]] || rc-service "$old_service" start || true
    else
        [[ ! -f "$backup/etc/systemd/system/ravel.service" ]] || cp -a "$backup/etc/systemd/system/ravel.service" /etc/systemd/system/ravel.service
        systemctl daemon-reload
        [[ -z "$old_service" ]] || systemctl start "$old_service.service" || true
    fi
    exit 1
}
mkdir -p "$directory" "$config_directory"
chmod 700 "$config_directory"
cp -a "$stage/config/." "$config_directory/"
find "$config_directory" -type f -exec chmod 600 {} +
install -m 755 "$stage/ravel" "$directory/ravel.new"
mv -f "$directory/ravel.new" "$directory/ravel"
install -m 755 "$stage/ravel.sh" /usr/local/bin/ravel
install -m 755 "$stage/install.sh" "$directory/install.sh.new"
mv -f "$directory/install.sh.new" "$directory/install.sh"
install -m 644 "$stage/geoip.dat" "$stage/geosite.dat" "$stage/LICENSE" "$stage/NOTICE" "$directory/"
if [[ $migrate == 1 ]]; then
    if [[ $manager == openrc ]]; then
        for service in v2node v2node-probe ravel-probe; do rc-service "$service" stop >/dev/null 2>&1 || true; done
    else
        systemctl stop v2node.service v2node-probe.service ravel-probe.service >/dev/null 2>&1 || true
    fi
fi
if [[ $manager == openrc ]]; then
    cat > /etc/init.d/ravel <<'UNIT'
#!/sbin/openrc-run
name="ravel"
description="Ravel runtime"
command="/usr/local/ravel/ravel"
command_args="run"
command_background="yes"
pidfile="/run/ravel.pid"
output_log="/var/log/ravel.log"
error_log="/var/log/ravel.log"
export GODEBUG=http2xconnect=1
depend() { need net; }
start_pre() { touch /var/log/ravel.log; chmod 600 /var/log/ravel.log; }
UNIT
    chmod 755 /etc/init.d/ravel
    rc-service ravel restart || rollback
    sleep 2
    rc-service ravel status || rollback
    rc-update add ravel default
else
    cat > /etc/systemd/system/ravel.service <<'UNIT'
[Unit]
Description=Ravel Runtime
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
ExecStart=/usr/local/ravel/ravel run
WorkingDirectory=/usr/local/ravel
Environment=GODEBUG=http2xconnect=1
UMask=0077
NoNewPrivileges=true
Restart=on-failure
RestartSec=5
[Install]
WantedBy=multi-user.target
UNIT
    systemctl daemon-reload
    systemctl restart ravel.service || rollback
    sleep 2
    systemctl is-active --quiet ravel.service || rollback
    systemctl enable ravel.service
fi
if [[ $migrate == 1 ]]; then
    if [[ $manager == openrc ]]; then
        for service in v2node v2node-probe ravel-probe; do rc-service "$service" stop >/dev/null 2>&1 || true; rc-update del "$service" default >/dev/null 2>&1 || true; done
    else
        systemctl disable --now v2node.service v2node-probe.service ravel-probe.service >/dev/null 2>&1 || true
    fi
fi
printf 'Ravel installed. Backup: %s\n' "$backup"
