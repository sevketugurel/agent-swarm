# Quota failover supervisor (external, provider-agnostic)

A deterministic watchdog that keeps an agent-swarm deployment working when a
model provider runs out of quota. It lives **outside** the swarm on purpose: a
failover implemented as an agent cannot run once the quota it is supposed to
manage is gone.

No LLM calls. Shell + HTTP API only. Roughly one tick per minute.

## What it does per tick

1. **Diagnose** — `GET /api/keys/status`. A provider counts as *blocked* only
   when every tracked key for it is `rate_limited` with `rateLimitedUntil` in
   the future.
2. **Go home first** — agents that were moved earlier and whose home provider is
   healthy again get their `HARNESS_PROVIDER`, `MODEL_OVERRIDE` and
   `REASONING_EFFORT_OVERRIDE` restored (all three: a model name is provider
   specific).
3. **Move** — only *idle* agents (`idle`, `offline`, `waiting_for_credentials`)
   are moved off a blocked provider onto one that still has capacity. The total
   number of agents parked on an alternate is capped, and a provider that was
   rate-limited in the last hour is treated as *freshly burned* and gets no new
   load.
4. **Recover tasks** — tasks whose `failureReason` is a quota error are
   re-dispatched with `routingReason: "reroute_fault"` and
   `parentTaskId` = the dead task, capped at 3 attempts per task, ~15 min apart.
   A task that failed for a real reason is never retried. A message mentioning
   `monthly` stops retries for that day.
5. **Wait** — if nothing has capacity it does nothing; the API flips
   `rateLimitedUntil` rows back to `available` on its own, so the next tick sees
   the window reopen without any polling logic of its own.

## Provider switch = one call

Use `PATCH /api/agents/<id>/runtime` with `{harness_provider, model,
reasoning_effort}` — the endpoint the dashboard uses. It validates the model
against the catalog and writes all three settings atomically. Writing
`MODEL_OVERRIDE` by hand (`PUT /api/config`) is silently reverted by the next
runtime report (the row comes back with `description="Set via PATCH
/api/agents/<id>/runtime"`), which quietly resets a codex agent to the
provider default model.

The "way home" of every moved agent is stored twice: in the local state file and
as a `FAILOVER_HOME` config row on the agent itself, so losing the state file
cannot strand an agent on the alternate provider. `reconcile` rebuilds state from
the swarm (markers + `failover-recovery` child tasks).

## Why this is not built into the swarm

- With several credentials in one env var, the worker asks
  `/api/keys/available` and picks a non-limited index — but when **all** keys are
  limited it still picks one and proceeds (`0/N keys available ... Selected N/N`).
  There is no wait-and-continue, so the request goes out and the task dies.
- The API already clears expired rate limits, so the "resume when the window
  reopens" half is free; the missing half is *not routing work to a provider
  whose window is exhausted*.

## Usage

```sh
# from the deployment checkout (reads API_KEY from ./.env)
python3 contrib/quota-failover/failover.py status        # read-only report
python3 contrib/quota-failover/failover.py tick          # dry run
python3 contrib/quota-failover/failover.py tick --apply   # one real pass
python3 contrib/quota-failover/test_failover.py          # 13 decision tests, no network
```

Schedule it (`contrib/quota-failover/failover_tick.sh` is the cron entry point):

```
* * * * * /path/to/contrib/quota-failover/failover_tick.sh
```

On macOS a launchd template is included; if both schedulers run, the `flock`
lock file makes the second one skip.

## Knobs

| Flag | Default | Meaning |
|---|---|---|
| `--alternates` | `codex` | alternate providers, preference order |
| `--alternate-capacity` | `2` | max agents parked on an alternate, total |
| `--burn-window` | `60` min | no new load on a provider limited within this window |
| `--max-moves` | `4` | provider switches per tick |
| `--dwell` | `900` s | minimum time before touching the same agent again |
| `--requeue-attempts` | `3` | re-dispatch attempts per task |
| `--requeue-retry-after` | `900` s | wait between attempts |
| `--requeue-hourly` | `4` | hourly re-dispatch budget |
| `--no-requeue` | off | disable task recovery |

State lives in `ops/.failover-state.json` next to the script (atomic write);
the lock file is `ops/.failover.lock`.

## Honest limit

One alternate account does not make a deployment continuous: failover moves the
wall, it does not remove it. To actually stay up you need either several
credentials per provider in the pool (comma separated env var) or a third cheap
lane (for example the `dsh` provider with a DeepSeek/OpenRouter key).

## Status of this directory

This is an external ops tool, not product code — it is deliberately dependency
free Python and is not wired into the Bun/TypeScript build. It exists here so the
pattern can be reviewed and, if useful, documented as a runbook.
