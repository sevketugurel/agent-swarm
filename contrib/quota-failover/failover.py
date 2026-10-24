#!/usr/bin/env python3
"""Agent Swarm autonomous quota failover supervisor.

Resident loop (launchd). Every tick it:

  1. reads the credential pool (/api/keys/status) and decides which providers
     are blocked (every tracked key rate-limited past now);
  2. moves idle agents off a blocked provider onto one that still has capacity
     (PATCH harness-provider + PUT MODEL_OVERRIDE), remembering their home
     provider/model;
  3. moves them home again once the provider's limit clears;
  4. re-dispatches tasks that died *because of* a quota error to an agent on a
     healthy provider (routingReason=reroute_fault), exactly once per task;
  5. when nothing has capacity it does nothing and keeps watching -- the loop
     is the wait, and the API clears rate_limited rows by itself when the
     window expires, so step 3 fires on its own.

State lives in ops/.failover-state.json. One log line per decision, so
launchd runs leave an audit trail in ops/failover.log.

Usage
-----
  python3 ops/failover.py status
  python3 ops/failover.py tick [--apply]      # one pass, dry-run by default
  python3 ops/failover.py watch --apply       # resident loop (launchd runs this)

Safety rails
------------
  --alternate-capacity N   max agents parked on one alternate provider (3)
  --max-moves N            max provider switches per tick (6)
  --dwell S                don't touch the same agent twice within S seconds
  --requeue-max N          max task re-dispatches per tick (2)
  --requeue-hourly N       max task re-dispatches per rolling hour (6)
  --requeue-max-age H      only recover tasks younger than H hours (12)
  --no-requeue             disable step 4
  --skip Name,Name         agents to leave alone
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_ENV = HERE.parent / ".env"
STATE_PATH = HERE / ".failover-state.json"
LOCK_PATH = HERE / ".failover.lock"
API_BASE = os.environ.get("SWARM_API", "http://localhost:3013")

# provider -> MODEL_OVERRIDE installed while that provider is in use.
# None = keep whatever the agent already carries.
PROVIDER_MODEL = {
    "claude": None,
    "codex": "gpt-6.1-sol",
    "dsh": "openrouter/deepseek/deepseek-v4.1-flash",
}
# Effort installed while parked on an alternate provider (claude agents keep
# their own effort on the way home).
ALTERNATE_EFFORT = "medium"

# An agent is only moved when it is not mid-task.
SWITCHABLE = {"idle", "offline", "waiting_for_credentials"}

# Where the "go home" information lives: a swarm_config row on the agent itself,
# so a lost/overwritten local state file cannot strand an agent on the alternate
# provider. Format: "<provider>|<model>|<effort>".
HOME_KEY = "FAILOVER_HOME"

# Move order: unblock the orchestrator first, then the lanes that produce work.
AGENT_PRIORITY = ["Lead", "Reviewer", "Tester", "Coder", "Researcher"]

# A failure counts as quota-caused only when the message looks like a provider
# limit -- not a test or a git push that merely mentions the words.
QUOTA_RE = re.compile(
    r"(hit your [\w\s'-]*limit|usage[ _-]?limit|session limit|quota exhausted"
    r"|insufficient_quota|too many requests|\b429\b|overloaded|rate.?limit)",
    re.I,
)
QUOTA_ATTRIBUTED_PREFIXES = (
    "Session error",
    "Runner error",
    "Provider error",
    "[usage-limit]",
    "usage-limit",
)
QUOTA_IGNORE_RE = re.compile(r"(test says|pytest|assert|git push returned)", re.I)
OPEN_TASK_STATES = ("pending", "assigned", "offered", "in_progress", "reviewing")


# ------------------------------------------------------------------- utilities


def now() -> datetime:
    return datetime.now(timezone.utc)


def log(msg: str) -> None:
    print(f"{now().strftime('%Y-%m-%dT%H:%M:%SZ')} {msg}", flush=True)


def parse_ts(value) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def api_key(env_path: Path) -> str:
    key = os.environ.get("SWARM_API_KEY")
    if key:
        return key
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("API_KEY="):
                return line.split("=", 1)[1].strip()
    sys.exit("API_KEY not found (env SWARM_API_KEY or .env)")


class Client:
    def __init__(self, base: str, key: str):
        self.base = base.rstrip("/")
        self.key = key

    def call(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.key}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                raw = r.read().decode()
                return r.status, (json.loads(raw) if raw else {})
        except urllib.error.HTTPError as e:
            raw = e.read().decode()
            try:
                return e.code, json.loads(raw)
            except Exception:
                return e.code, {"raw": raw[:300]}
        except Exception as e:
            return 0, {"error": str(e)}


# --------------------------------------------------------------- swarm reading


def apply_quota_storm(pstate: dict, tasks: list[dict], threshold: int = 3, window_min: int = 60) -> dict:
    """Mark providers that are failing tasks on quota even though keys look free.

    This is the shadowed-pool case: the API's key rows say "available" but every
    run dies on a quota error because the worker is using a different (exhausted)
    credential. Without this signal the supervisor keeps agents on that provider
    and the failure repeats once a minute.
    """
    cutoff = now() - timedelta(minutes=window_min)
    counts: dict[str, int] = {}
    for t in tasks:
        if t.get("status") != "failed":
            continue
        stamp = parse_ts(t.get("finishedAt") or t.get("createdAt"))
        if not stamp or stamp < cutoff:
            continue
        if not quota_failure(t.get("failureReason") or ""):
            continue
        prov = t.get("provider") or ""
        if prov:
            counts[prov] = counts.get(prov, 0) + 1
    storms: dict[str, int] = {}
    for prov, n in counts.items():
        if n < threshold:
            continue
        slot = pstate.setdefault(
            prov,
            {"keys": 0, "limited": 0, "blocked": False, "until": None,
             "until_iso": None, "last_hit": None},
        )
        slot["storm"] = n
        slot["blocked"] = True
        slot["until_iso"] = slot.get("until_iso") or f"quota-storm({n}/h)"
        storms[prov] = n
    return storms


def provider_state(keys: list[dict]) -> dict[str, dict]:
    """providers -> {keys, limited, blocked, until, until_iso}.

    A provider is blocked only when it has at least one tracked key and every
    one of them is rate-limited past now. Providers with no tracked key rows
    report keys=0, blocked=False so callers can require a positive signal.
    """
    out: dict[str, dict] = {}
    for k in keys:
        prov = k.get("provider") or "unknown"
        slot = out.setdefault(prov, {"keys": 0, "limited": 0, "until": None, "last_hit": None})
        slot["keys"] += 1
        until = parse_ts(k.get("rateLimitedUntil"))
        last_hit = parse_ts(k.get("lastRateLimitAt"))
        if last_hit and (slot["last_hit"] is None or last_hit > slot["last_hit"]):
            slot["last_hit"] = last_hit
        limited = k.get("status") == "rate_limited" and (until is None or until > now())
        if limited:
            slot["limited"] += 1
            if until and (slot["until"] is None or until > slot["until"]):
                slot["until"] = until
    for slot in out.values():
        slot["blocked"] = slot["keys"] > 0 and slot["limited"] == slot["keys"]
        slot["until_iso"] = slot["until"].isoformat() if slot["until"] else None
        slot["last_hit_iso"] = slot["last_hit"].isoformat() if slot["last_hit"] else None
    return out


def agent_provider(a: dict) -> str:
    return a.get("harnessProvider") or a.get("provider") or ""


def fetch_resolved(c: Client, agent_id: str) -> dict:
    """Merged config for one agent as a flat KEY -> value dict."""
    status, body = c.call("GET", f"/api/config/resolved?agentId={agent_id}")
    if status != 200:
        return {}
    for key in ("configs", "config", "resolved", "entries"):
        val = body.get(key)
        if isinstance(val, dict):
            return val
        if isinstance(val, list):
            flat: dict = {}
            for e in val:
                if isinstance(e, dict) and e.get("key") not in flat:
                    flat[e.get("key")] = e.get("value")
            return flat
    return {}


def load_state() -> dict:
    state: dict = {}
    for path in (STATE_PATH, STATE_PATH.with_suffix(".bak")):
        if not path.exists():
            continue
        try:
            state = json.loads(path.read_text())
            if path is not STATE_PATH:
                log(f"state: {STATE_PATH.name} unreadable, recovered from backup")
            break
        except Exception:
            state = {}
    state.setdefault("agents", {})
    state.setdefault("requeued", {})
    state.setdefault("last_move", {})
    state.setdefault("requeue_times", [])
    return state


def save_state(state: dict) -> None:
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    if STATE_PATH.exists():
        try:
            STATE_PATH.replace(STATE_PATH.with_suffix(".bak"))
        except OSError:
            pass
    tmp.replace(STATE_PATH)


def adopt_home_markers(c: Client, state: dict, agents: list[dict], alternates: list[str]) -> list[str]:
    """Re-adopt agents that carry a FAILOVER_HOME marker but are missing from state.

    Protects against a lost or overwritten local state file: the marker lives on
    the agent's own config, so the way home survives.
    """
    adopted: list[str] = []
    for a in agents:
        if a["id"] in state["agents"]:
            continue
        if agent_provider(a) not in alternates:
            continue
        marker = fetch_resolved(c, a["id"]).get(HOME_KEY)
        if not marker or not isinstance(marker, str) or "|" not in marker:
            continue
        prov, model, effort = (marker.split("|") + ["", ""])[:3]
        state["agents"][a["id"]] = {
            "primary_provider": prov or "claude",
            "primary_model": model or None,
            "primary_effort": effort or None,
            "switched_at": now().isoformat(),
            "adopted": True,
        }
        adopted.append(f"{a['name']} (home={prov})")
    if adopted:
        save_state(state)
    return adopted


def mark_home(c: Client, agent: dict, provider: str, model: str | None, effort: str | None) -> bool:
    value = "|".join([provider, model or "", effort or ""])
    return put_config(c, agent["id"], HOME_KEY, value, "failover: where this agent belongs")


def clear_home(c: Client, agent: dict) -> None:
    status, body = c.call("GET", f"/api/config?scope=agent&scopeId={agent['id']}")
    if status != 200:
        return
    for row in body.get("configs", []):
        if row.get("key") == HOME_KEY:
            c.call("DELETE", f"/api/config/{row['id']}")


def rank(name: str) -> tuple:
    for i, pref in enumerate(AGENT_PRIORITY):
        if name.startswith(pref):
            return (i, name)
    return (len(AGENT_PRIORITY), name)


def acquire_lock():
    """Single-instance guard: cron ticks must never overlap a watch loop."""
    fh = open(LOCK_PATH, "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        return None
    fh.write(str(os.getpid()))
    fh.flush()
    return fh


# ---------------------------------------------------------------------- writes


def put_config(c: Client, agent_id: str, key: str, value: str, description: str) -> bool:
    status, body = c.call(
        "PUT",
        "/api/config",
        {
            "scope": "agent",
            "scopeId": agent_id,
            "key": key,
            "value": value,
            "isSecret": False,
            "exportToEnv": True,
            "description": description,
        },
    )
    return status in (200, 201)


def switch(
    c: Client,
    agent: dict,
    provider: str,
    model: str | None,
    effort: str | None = None,
) -> bool:
    """Change an agent's provider + model + effort through the supported endpoint.

    PATCH /api/agents/<id>/runtime is the same call the dashboard makes: it
    updates `agents.harness_provider` and the agent-scoped runtime rows in one
    transaction and validates the model against the catalog. Writing
    MODEL_OVERRIDE by hand (PUT /api/config) gets overwritten by the next
    runtime report, so we never do that for provider switches.
    """
    body: dict = {"harness_provider": provider}
    if model is not None:
        body["model"] = model
    if effort is not None:
        body["reasoning_effort"] = effort
    status, payload = c.call("PATCH", f"/api/agents/{agent['id']}/runtime", body)
    if status != 200:
        log(f"  ! {agent['name']}: runtime patch failed {status} {str(payload)[:200]}")
        return False
    return True


# -------------------------------------------------------------------- planning


def plan_routing(args, c: Client, state: dict, pstate: dict, agents: list[dict]) -> list[str]:
    actions: list[str] = []
    ts = now()
    # Count every agent currently sitting on an alternate provider (including
    # ones we moved earlier this hour and agents whose home is the alternate),
    # so the cap cannot creep upward tick after tick.
    parked = sum(1 for a in agents if agent_provider(a) in args.alternates)

    def can_touch(a: dict) -> bool:
        last = parse_ts(state["last_move"].get(a["id"]))
        return last is None or (ts - last).total_seconds() >= args.dwell

    def healthy(prov: str) -> bool:
        s = pstate.get(prov) or {}
        if s.get("keys", 0) <= 0 or s.get("blocked"):
            return False
        # freshly-burned account: even if the window reopened, parking more
        # agents on it just re-burns it (burn_window minutes of cooldown)
        hit = s.get("last_hit")
        if hit and (ts - hit).total_seconds() < args.burn_window * 60:
            return False
        return True

    # 1) go home: primary provider is healthy again
    for a in sorted(agents, key=lambda x: rank(x["name"])):
        rec = state["agents"].get(a["id"])
        if not rec or a["status"] not in SWITCHABLE or not can_touch(a):
            continue
        if rec.get("hold"):
            until = parse_ts(rec.get("hold_until"))
            if until is None or until > now():
                continue  # deliberately pinned on the alternate (optionally until a time)
        home = rec["primary_provider"]
        if pstate.get(home, {}).get("blocked"):
            continue
        if not (
            args.apply
            and switch(c, a, home, rec.get("primary_model"), rec.get("primary_effort"))
        ):
            if args.apply:
                continue
        actions.append(f"HOME    {a['name']}: {agent_provider(a)} -> {home}")
        if args.apply:
            clear_home(c, a)
            state["agents"].pop(a["id"], None)
            state["last_move"][a["id"]] = ts.isoformat()
            save_state(state)
            parked -= 1

    # 2) leave a blocked provider when capacity exists elsewhere
    swaps = 0
    for a in sorted(agents, key=lambda x: rank(x["name"])):
        if any(a["name"].startswith(s) for s in args.skip):
            continue
        if a["id"] in state["agents"] or a["status"] not in SWITCHABLE or not can_touch(a):
            continue
        cur = agent_provider(a)
        if not pstate.get(cur, {}).get("blocked"):
            continue
        target = next((p for p in args.alternates if p != cur and healthy(p)), None)
        if target is None:
            continue
        if swaps >= args.max_moves:
            continue
        if parked >= args.alternate_capacity:
            actions.append(
                f"HOLD    {a['name']}: alternate capacity reached ({args.alternate_capacity})"
            )
            continue
        model = PROVIDER_MODEL.get(target)
        resolved = fetch_resolved(c, a["id"])
        home_model = resolved.get("MODEL_OVERRIDE") or None
        home_effort = resolved.get("REASONING_EFFORT_OVERRIDE") or None
        if args.apply and not switch(c, a, target, model, ALTERNATE_EFFORT):
            continue
        actions.append(
            f"SWAP    {a['name']}: {cur} -> {target} (model={model or 'kept'}, effort={ALTERNATE_EFFORT})"
        )
        parked += 1
        swaps += 1
        if args.apply:
            mark_home(c, a, cur, home_model, home_effort)
            state["agents"][a["id"]] = {
                "primary_provider": cur,
                "primary_model": home_model,
                "primary_effort": home_effort,
                "switched_at": ts.isoformat(),
            }
            state["last_move"][a["id"]] = ts.isoformat()
            save_state(state)
    return actions


def quota_failure(reason: str) -> bool:
    if not reason or len(reason) > 800:
        return False
    if QUOTA_IGNORE_RE.search(reason) or not QUOTA_RE.search(reason):
        return False
    return reason.strip().startswith(QUOTA_ATTRIBUTED_PREFIXES)


def plan_recovery(args, c: Client, state: dict, pstate: dict, tasks: list[dict], agents: list[dict]) -> list[str]:
    """Re-dispatch quota-killed tasks, bounded retries, to a healthy provider.

    A task is retried only while every previous attempt died on a quota error
    (a real failure stops it), at most --requeue-attempts times, and never onto
    a provider whose window is blocked or freshly burned.
    """
    if args.no_requeue:
        return []
    hour_ago = now() - timedelta(hours=1)
    state["requeue_times"] = [t for t in state["requeue_times"] if (parse_ts(t) or hour_ago) > hour_ago]
    budget = min(args.requeue_max, args.requeue_hourly - len(state["requeue_times"]))
    if budget <= 0:
        return []

    def provider_usable(prov: str) -> bool:
        s = pstate.get(prov) or {}
        if s.get("keys", 0) <= 0 or s.get("blocked"):
            return False
        hit = s.get("last_hit")
        return not (hit and (now() - hit).total_seconds() < args.burn_window * 60)

    healthy_agents = [a for a in agents if provider_usable(agent_provider(a))]
    if not healthy_agents:
        return []
    busy: dict[str, int] = {}
    children: dict[str, list[dict]] = {}
    for t in tasks:
        if t.get("status") in OPEN_TASK_STATES:
            busy[t.get("agentId")] = busy.get(t.get("agentId"), 0) + 1
        if t.get("parentTaskId"):
            children.setdefault(t["parentTaskId"], []).append(t)
    by_task = {t["id"]: t for t in tasks}
    # tasks produced by this supervisor: they are evaluated only through their
    # parent, never as a fresh original (otherwise retries chain into copies)
    recovery_children = {str(r.get("new") or "") for r in state["requeued"].values()}
    cutoff = now() - timedelta(hours=args.requeue_max_age)
    actions: list[str] = []

    for t in sorted(tasks, key=lambda x: x.get("createdAt") or ""):
        if len(actions) >= budget:
            break
        if t.get("status") != "failed":
            continue
        created = parse_ts(t.get("createdAt"))
        if created and created < cutoff:
            continue

        rec = state["requeued"].get(t["id"])
        if rec:
            attempts = int(rec.get("attempts", 1))
            if attempts >= args.requeue_attempts:
                continue
            child = by_task.get(str(rec.get("new") or ""))
            if not child or child.get("status") != "failed":
                continue  # still queued, running, or it succeeded -> leave alone
            child_reason = child.get("failureReason") or ""
            if not quota_failure(child_reason):
                continue  # died of something real; retrying would only waste quota
            if (now() - (parse_ts(rec.get("at")) or now())).total_seconds() < args.requeue_retry_after:
                continue
        else:
            attempts = 0
            if t["id"] in recovery_children or "failover-recovery" in (t.get("tags") or []):
                continue  # our own continuation copy; handled via its parent
            if not quota_failure(t.get("failureReason") or ""):
                continue
            kids = children.get(t["id"], [])
            if any(k.get("status") in OPEN_TASK_STATES or k.get("status") == "completed" for k in kids):
                continue

        same_agent = [a for a in healthy_agents if a["id"] == t.get("agentId")]
        if same_agent and same_agent[0].get("status") == "idle":
            target = same_agent[0]
        else:
            # worker work should not pile onto the Lead: only fall back to it
            # when no other idle agent on a healthy provider exists
            workers = [
                a
                for a in healthy_agents
                if a.get("status") == "idle" and not a["name"].startswith("Lead")
            ]
            idle = workers or [a for a in healthy_agents if a.get("status") == "idle"]
            if not idle:
                continue
            target = sorted(idle, key=lambda a: (busy.get(a["id"], 0), rank(a["name"])))[0]
        original = (t.get("task") or "").strip()
        prompt = (
            "Provider failover continuation. The original task below failed because its model "
            f"provider ({t.get('provider') or 'unknown'}) hit a rate/quota limit, not because of the "
            "work itself. Re-check the current repository/workspace state first and reuse anything "
            "that already exists -- do not redo finished steps. Then finish the task.\n\n"
            f"--- ORIGINAL TASK (task id {t['id']}) ---\n{original[:12000]}"
        )
        payload = {
            "task": prompt,
            "agentId": target["id"],
            "parentTaskId": t["id"],
            "routingReason": "reroute_fault",
            "routingNote": "provider rate-limit failover (ops/failover.py)",
            "taskType": "follow-up",
            "tags": ["failover-recovery"],
            "priority": max(50, min(90, int(t.get("priority") or 50))),
        }
        if not args.apply:
            label = f"attempt {attempts + 1}/{args.requeue_attempts}"
            actions.append(f"REQUEUE task {t['id'][:8]} -> {target['name']} ({label}, dry-run)")
            continue
        status, body = c.call("POST", "/api/tasks", payload)
        if status in (200, 201):
            # POST /api/tasks returns the AgentTask itself; its "task" field is the text
            new_id = str((body or {}).get("id") or "?")
            state["requeued"][t["id"]] = {
                "at": now().isoformat(),
                "to": target["id"],
                "new": new_id,
                "attempts": attempts + 1,
            }
            state["requeue_times"].append(now().isoformat())
            save_state(state)
            actions.append(
                f"REQUEUE task {t['id'][:8]} -> {target['name']} "
                f"(new {new_id[:8]}, attempt {attempts + 1}/{args.requeue_attempts})"
            )
        else:
            log(f"  ! requeue {t['id'][:8]} failed {status} {str(body)[:200]}")
    return actions


# -------------------------------------------------------------------- commands


def snapshot(c: Client):
    _, keys = c.call("GET", "/api/keys/status")
    _, agents = c.call("GET", "/api/agents")
    # fields=full is required: the default "slim" shape drops failureReason
    _, tasks = c.call("GET", "/api/tasks?limit=500&fields=full")
    return (
        keys.get("keys", []),
        provider_state(keys.get("keys", [])),
        agents.get("agents", []),
        tasks.get("tasks", []),
    )


def cmd_reconcile(c: Client, args) -> int:
    """Rebuild local state from the swarm: FAILOVER_HOME markers + recovery tags."""
    keys, pstate, agents, tasks = snapshot(c)
    state = load_state()
    task_ids = {t["id"] for t in tasks}
    for pid in [p for p, r in state["requeued"].items() if str(r.get("new")) not in task_ids]:
        state["requeued"].pop(pid, None)
        print(f"  dropped stale recovery record: {pid[:8]} (child not in swarm)")
    for name in adopt_home_markers(c, state, agents, args.alternates):
        print(f"  adopted from marker: {name}")
    added = 0
    for t in tasks:
        if "failover-recovery" not in (t.get("tags") or []):
            continue
        parent = t.get("parentTaskId")
        if not parent:
            continue
        rec = state["requeued"].get(parent)
        if rec and int(rec.get("attempts", 1)) >= 1:
            continue
        state["requeued"][parent] = {
            "at": t.get("createdAt") or now().isoformat(),
            "to": t.get("agentId"),
            "new": t["id"],
            "attempts": 1,
            "reconciled": True,
        }
        added += 1
        print(f"  recovery record: parent {parent[:8]} -> child {t['id'][:8]} ({t.get('status')})")
    save_state(state)
    print(
        f"state: {len(state['agents'])} swap record(s), "
        f"{len(state['requeued'])} recovery record(s) ({added} new)"
    )
    return 0


def cmd_status(c: Client, args) -> int:
    keys, pstate, agents, tasks = snapshot(c)
    state = load_state()
    print("CREDENTIAL POOL")
    for k in keys:
        print(
            f"  {k['keyType']:26} ...{k.get('keySuffix','')}  {k['status']:13}"
            f" until={k.get('rateLimitedUntil') or '-'}  hits={k.get('rateLimitCount')}"
        )
    print("PROVIDER VIEW")
    for p, s in sorted(pstate.items()):
        print(
            f"  {p:10} {'BLOCKED' if s['blocked'] else 'ok':8} keys={s['keys']}"
            f" limited={s['limited']} until={s['until_iso'] or '-'}"
        )
    print("AGENTS")
    for a in sorted(agents, key=lambda x: rank(x["name"])):
        home = (state["agents"].get(a["id"]) or {}).get("primary_provider", "")
        note = f"  (swapped, home={home})" if home else ""
        print(f"  {a['name'][:26]:28} {agent_provider(a):7} status={a['status']}{note}")
    dead = [t for t in tasks if t.get("status") == "failed" and quota_failure(t.get("failureReason") or "")]
    print(f"QUOTA-FAILED TASKS: {len(dead)}   requeued so far: {len(state['requeued'])}")
    return 0


def one_tick(c: Client, args) -> int:
    keys, pstate, agents, tasks = snapshot(c)
    if not keys or not agents:
        # one retry: the API can be mid-restart or busy for a moment
        time.sleep(5)
        keys, pstate, agents, tasks = snapshot(c)
    if not keys or not agents:
        log("tick: swarm API unreachable, skipping")
        return 1
    state = load_state()
    storms = apply_quota_storm(pstate, tasks, args.storm_threshold)
    for prov, n in storms.items():
        log(f"STORM   {prov}: {n} kota hatası/saat — anahtarlar 'available' görünse de blokli sayılıyor")
    for name in adopt_home_markers(c, state, agents, args.alternates):
        log(f"STATE   re-adopted {name} from FAILOVER_HOME marker")
    actions = plan_routing(args, c, state, pstate, agents)
    actions += plan_recovery(args, c, state, pstate, tasks, agents)
    if actions:
        tag = "APPLY" if args.apply else "DRY"
        for a in actions:
            log(f"{tag}  {a}")
    else:
        summary = ", ".join(
            f"{p}={'blocked until ' + str(s['until_iso']) if s['blocked'] else 'ok'}"
            for p, s in sorted(pstate.items())
        )
        log(f"tick: no action ({summary})")
    return 0


def cmd_watch(c: Client, args) -> int:
    log(
        f"watch: start interval={args.interval}s apply={args.apply} "
        f"alternates={','.join(args.alternates)} capacity={args.alternate_capacity}"
    )
    while True:
        try:
            one_tick(c, args)
        except KeyboardInterrupt:
            log("watch: stopped")
            return 0
        except Exception as e:  # a supervisor that dies is worse than one that logs
            log(f"watch: tick error {type(e).__name__}: {e}")
        time.sleep(args.interval)


def main() -> int:
    ap = argparse.ArgumentParser(description="Autonomous swarm quota failover")
    ap.add_argument("command", choices=["status", "tick", "watch", "reconcile"])
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--env", default=str(DEFAULT_ENV))
    ap.add_argument("--alternates", default="codex")
    ap.add_argument("--alternate-capacity", type=int, default=2)
    ap.add_argument("--burn-window", type=float, default=60.0, help="minutes to avoid a freshly burned provider")
    ap.add_argument("--storm-threshold", type=int, default=3,
                    help="quota failures in the last hour that mark a provider unhealthy despite 'available' keys")
    ap.add_argument("--max-moves", type=int, default=4)
    ap.add_argument("--dwell", type=int, default=900)
    ap.add_argument("--interval", type=int, default=60)
    ap.add_argument("--requeue-max", type=int, default=2)
    ap.add_argument("--requeue-hourly", type=int, default=4)
    ap.add_argument("--requeue-attempts", type=int, default=3)
    ap.add_argument("--requeue-retry-after", type=int, default=900)
    ap.add_argument("--requeue-max-age", type=float, default=12.0)
    ap.add_argument("--no-requeue", action="store_true")
    ap.add_argument("--skip", default="")
    args = ap.parse_args()
    args.alternates = [p.strip() for p in args.alternates.split(",") if p.strip()]
    args.skip = tuple(s.strip() for s in args.skip.split(",") if s.strip())

    c = Client(API_BASE, api_key(Path(args.env)))
    if args.command == "status":
        return cmd_status(c, args)
    if args.command == "reconcile":
        return cmd_reconcile(c, args)
    lock = acquire_lock()
    if lock is None:
        log("another failover instance is running, skipping this tick")
        return 0
    if args.command == "tick":
        return one_tick(c, args)
    return cmd_watch(c, args)


if __name__ == "__main__":
    sys.exit(main())
