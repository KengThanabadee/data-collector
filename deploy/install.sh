#!/usr/bin/env bash
# Prepare a supplied immutable release. Does not enable timers or buy resources.
set -euo pipefail
if [[ ${EUID} -ne 0 ]]; then
  echo 'Run as root on the target Ubuntu VPS.' >&2
  exit 1
fi
if [[ $(uname -s) != Linux || $# -ne 1 ]]; then
  echo 'Usage: sudo bash deploy/install.sh /opt/option-collector/releases/VERSION' >&2
  exit 1
fi
collector_release=$(cd -- "$1" && pwd -P)
case "$collector_release" in
  /opt/option-collector/releases/*) ;;
  *) echo 'Release must be under /opt/option-collector/releases.' >&2; exit 1 ;;
esac
for collector_unit in option-collector.timer option-delivery.timer option-collector.service option-delivery.service; do
  if systemctl is-active --quiet "$collector_unit"; then
    echo "Pause and wait for $collector_unit before installing." >&2
    exit 1
  fi
done
test -f "$collector_release/requirements.lock"
test -f "$collector_release/option_collector/cli.py"
python3.12 -c 'import sys; assert sys.version_info[:2] == (3,12)'
if ! id option-collector >/dev/null 2>&1; then
  useradd --system --user-group --home-dir /nonexistent --shell /usr/sbin/nologin option-collector
fi
if [[ $(id -u option-collector) -eq 0 ]]; then exit 1; fi
install -d -m 0750 -o option-collector -g option-collector /var/lib/option-collector
install -d -m 0750 -o root -g option-collector /etc/option-collector
if [[ ! -e /etc/option-collector/collector.toml ]]; then
  install -m 0640 -o root -g option-collector "$collector_release/config/vps.toml" /etc/option-collector/collector.toml
fi
if [[ ! -e /etc/option-collector/healthchecks.json ]]; then
  printf '{}\n' >/etc/option-collector/healthchecks.json
  chown option-collector:option-collector /etc/option-collector/healthchecks.json
  chmod 0600 /etc/option-collector/healthchecks.json
fi
python3.12 -m venv "$collector_release/.venv"
"$collector_release/.venv/bin/python" -m pip install --no-cache-dir -r "$collector_release/requirements.lock"
chgrp -R option-collector "$collector_release"
chmod -R g+rX "$collector_release"
for collector_file in "$collector_release"/deploy/systemd/*; do
  install -m 0644 "$collector_file" /etc/systemd/system/
done
install -m 0644 "$collector_release/deploy/journald@option-collector.conf" /etc/systemd/journald@option-collector.conf
if [[ ! -e /opt/option-collector/current ]]; then
  ln -s "$collector_release" /opt/option-collector/current
else
  echo 'Existing current release preserved. Switch its symlink explicitly after validation.'
fi
systemctl daemon-reload
echo 'Installed files only. Validate Linux behavior and Telegram before enabling timers.'
