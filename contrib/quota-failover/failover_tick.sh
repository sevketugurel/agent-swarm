#!/bin/bash
# Tek tur kota bekçisi. Cron/launchd bu betiği çağırır; çıktı ops/failover.log'a gider.
# Çalışma dizini: AGENT_SWARM_WORKSPACE (varsayılan $HOME/agent-swarm-workspace)
set -u

WORKSPACE="${AGENT_SWARM_WORKSPACE:-$HOME/agent-swarm-workspace}"
[ -d "$WORKSPACE" ] || exit 0
cd "$WORKSPACE" || exit 1
exec /usr/bin/python3 ops/failover.py tick --apply >> ops/failover.log 2>&1
