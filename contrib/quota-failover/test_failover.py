#!/usr/bin/env python3
"""Decision-logic tests for ops/failover.py -- no network, no swarm touched.

Run: /usr/bin/python3 ops/test_failover.py
"""

import importlib.util
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

spec = importlib.util.spec_from_file_location("fo", Path(__file__).with_name("failover.py"))
fo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fo)

# tests must never write into the repository: state and lock go to a temp dir
_TMP = Path(tempfile.mkdtemp(prefix="failover-test-"))
fo.STATE_PATH = _TMP / "state.json"
fo.LOCK_PATH = _TMP / "lock"

NOW = datetime.now(timezone.utc)
AG = {"id": "a1", "name": "Worker One", "harnessProvider": "claude", "status": "idle"}
AG2 = {"id": "a2", "name": "Worker Two", "harnessProvider": "claude", "status": "idle"}
AG_BUSY = {"id": "a3", "name": "Worker Three", "harnessProvider": "claude", "status": "busy"}
FAKE_ARGS = fo.argparse.Namespace(
    apply=True,
    alternates=["codex"],
    alternate_capacity=2,
    burn_window=60.0,
    max_moves=4,
    dwell=900,
    requeue_max=2,
    requeue_hourly=4,
    requeue_attempts=3,
    requeue_retry_after=900,
    requeue_max_age=12.0,
    no_requeue=False,
    skip=(),
    env=".env",
    interval=60,
)


class Fake:
    def __init__(self, resolved=None):
        self.calls = []
        self.resolved = resolved or {
            "MODEL_OVERRIDE": "claude-sonnet-5-5",
            "REASONING_EFFORT_OVERRIDE": "medium",
        }

    def call(self, method, path, body=None):
        self.calls.append((method, path, body))
        if path.startswith("/api/config/resolved"):
            return 200, {"configs": [{"key": k, "value": v} for k, v in self.resolved.items()]}
        if path.startswith("/api/tasks"):
            return 201, {"id": f"new-{len(self.calls)}"}
        return 200, {}

    def switched(self):
        return [(p.split("/")[3], b) for m, p, b in self.calls if m == "PATCH" and "/runtime" in p]

    def config_writes(self):
        return [(b["key"], b["value"]) for m, p, b in self.calls if m == "PUT"]


def pstate(claude_blocked_until=None, codex_blocked_until=None, codex_hit=None):
    def slot(until, hit=None):
        blocked = until is not None and until > NOW
        return {
            "keys": 1,
            "limited": 1 if blocked else 0,
            "blocked": blocked,
            "until": until,
            "until_iso": until.isoformat() if until else None,
            "last_hit": hit,
        }

    return {
        "claude": slot(claude_blocked_until, None),
        "codex": slot(codex_blocked_until, codex_hit),
    }


def run(name, fn):
    try:
        fn()
        print(f"PASS  {name}")
        return True
    except AssertionError as e:
        print(f"FAIL  {name}: {e}")
        return False


def t_swap_when_home_blocked():
    c, state = Fake(), {"agents": {}, "requeued": {}, "last_move": {}, "requeue_times": []}
    acts = fo.plan_routing(FAKE_ARGS, c, state, pstate(claude_blocked_until=NOW + timedelta(hours=5)), [AG])
    calls = c.switched()
    assert len(calls) == 1 and calls[0][0] == "a1", calls
    body = calls[0][1]
    assert body["harness_provider"] == "codex", body
    assert body["model"] == "gpt-6.1-sol", body
    assert body["reasoning_effort"] == "medium", body
    assert ("FAILOVER_HOME", "claude|claude-sonnet-5-5|medium") in c.config_writes(), c.config_writes()
    assert state["agents"]["a1"]["primary_provider"] == "claude"
    assert state["agents"]["a1"]["primary_model"] == "claude-sonnet-5-5"
    assert state["agents"]["a1"]["primary_effort"] == "medium"
    assert any(x.startswith("SWAP") for x in acts), acts


def t_no_swap_when_home_healthy():
    c = Fake()
    state = {"agents": {}, "requeued": {}, "last_move": {}, "requeue_times": []}
    acts = fo.plan_routing(FAKE_ARGS, c, state, pstate(), [AG])
    assert c.switched() == [], c.switched()
    assert acts == [], acts


def t_capacity_holds():
    c = Fake()
    state = {"agents": {}, "requeued": {}, "last_move": {}, "requeue_times": []}
    acts = fo.plan_routing(
        FAKE_ARGS, c, state, pstate(claude_blocked_until=NOW + timedelta(hours=5)), [AG, AG2]
    )
    assert len(c.switched()) == 2, c.switched()  # capacity is 2 -> both fit exactly
    state2 = {"agents": {}, "requeued": {}, "last_move": {}, "requeue_times": []}
    strict = fo.argparse.Namespace(**{**vars(FAKE_ARGS), "alternate_capacity": 1})
    c2 = Fake()
    fo.plan_routing(strict, c2, state2, pstate(claude_blocked_until=NOW + timedelta(hours=5)), [AG, AG2])
    assert len(c2.switched()) == 1, c2.switched()
    assert any(x.startswith("HOLD") for x in fo.plan_routing(
        strict, Fake(), {"agents": {}, "requeued": {}, "last_move": {}, "requeue_times": []},
        pstate(claude_blocked_until=NOW + timedelta(hours=5)), [AG, AG2]))


def t_busy_agent_untouched():
    c = Fake()
    state = {"agents": {}, "requeued": {}, "last_move": {}, "requeue_times": []}
    fo.plan_routing(FAKE_ARGS, c, state, pstate(claude_blocked_until=NOW + timedelta(hours=5)), [AG_BUSY])
    assert c.switched() == [], c.switched()


def t_burned_alternate_skipped():
    c = Fake()
    state = {"agents": {}, "requeued": {}, "last_move": {}, "requeue_times": []}
    fo.plan_routing(
        FAKE_ARGS, c, state,
        pstate(claude_blocked_until=NOW + timedelta(hours=5), codex_hit=NOW - timedelta(minutes=10)),
        [AG],
    )
    assert c.switched() == [], c.switched()


def t_home_when_limit_clears():
    c = Fake()
    state = {
        "agents": {"a1": {"primary_provider": "claude", "primary_model": "claude-sonnet-5-5",
                          "primary_effort": "high", "switched_at": NOW.isoformat()}},
        "requeued": {}, "last_move": {"a1": (NOW - timedelta(hours=1)).isoformat()}, "requeue_times": [],
    }
    acts = fo.plan_routing(FAKE_ARGS, c, state, pstate(), [AG])
    calls = c.switched()
    assert len(calls) == 1, calls
    body = calls[0][1]
    assert body == {"harness_provider": "claude", "model": "claude-sonnet-5-5",
                    "reasoning_effort": "high"}, body
    assert "a1" not in state["agents"], state["agents"]
    assert any(x.startswith("HOME") for x in acts), acts


def t_dwell_blocks_flapping():
    c = Fake()
    state = {
        "agents": {"a1": {"primary_provider": "claude", "primary_model": "claude-sonnet-5-5",
                          "primary_effort": "medium"}},
        "requeued": {}, "last_move": {"a1": NOW.isoformat()}, "requeue_times": [],
    }
    fo.plan_routing(FAKE_ARGS, c, state, pstate(), [AG])
    assert c.calls == [], c.calls


FAILED_TASK = {
    "id": "t1", "status": "failed", "createdAt": (NOW - timedelta(hours=1)).isoformat(),
    "agentId": "a9", "provider": "claude", "task": "Write the report", "priority": 60,
    "failureReason": "Session error (exit code 1): You've hit your weekly limit · resets 8am (UTC)",
}


def t_requeue_quota_failure():
    c = Fake()
    state = {"agents": {}, "requeued": {}, "last_move": {}, "requeue_times": []}
    acts = fo.plan_recovery(FAKE_ARGS, c, state, pstate(), [FAILED_TASK], [{**AG, "id": "a9"}])
    assert len(acts) == 1 and acts[0].startswith("REQUEUE"), acts
    assert state["requeued"]["t1"]["attempts"] == 1, state["requeued"]
    post = [b for m, p, b in c.calls if m == "POST"]
    assert post and post[0]["routingReason"] == "reroute_fault", post
    assert post[0]["parentTaskId"] == "t1"


def t_requeue_skips_real_failure():
    c = Fake()
    state = {"agents": {}, "requeued": {}, "last_move": {}, "requeue_times": []}
    bad = {**FAILED_TASK, "failureReason": "Session error (exit code 1): pytest failed: 3 tests"}
    acts = fo.plan_recovery(FAKE_ARGS, c, state, pstate(), [bad], [{**AG, "id": "a9"}])
    assert acts == [], acts


def t_requeue_retries_when_child_died_on_quota():
    c = Fake()
    state = {
        "agents": {}, "last_move": {}, "requeue_times": [],
        "requeued": {"t1": {"at": (NOW - timedelta(minutes=30)).isoformat(), "to": "a9",
                            "new": "c1", "attempts": 1}},
    }
    child = {"id": "c1", "status": "failed", "parentTaskId": "t1", "createdAt": NOW.isoformat(),
             "failureReason": "[usage-limit] Codex account quota exhausted — wait for reset"}
    tasks = [FAILED_TASK, child]
    acts = fo.plan_recovery(FAKE_ARGS, c, state, pstate(), tasks, [{**AG, "id": "a9"}])
    assert len(acts) == 1, acts
    assert state["requeued"]["t1"]["attempts"] == 2, state["requeued"]


def t_requeue_stops_after_max_attempts():
    c = Fake()
    state = {
        "agents": {}, "last_move": {}, "requeue_times": [],
        "requeued": {"t1": {"at": (NOW - timedelta(hours=2)).isoformat(), "to": "a9",
                            "new": "c1", "attempts": 3}},
    }
    child = {"id": "c1", "status": "failed", "parentTaskId": "t1", "createdAt": NOW.isoformat(),
             "failureReason": "[usage-limit] Codex account quota exhausted"}
    acts = fo.plan_recovery(FAKE_ARGS, c, state, pstate(), [FAILED_TASK, child], [{**AG, "id": "a9"}])
    assert acts == [], acts


def t_adopts_home_marker_when_state_lost():
    """State dosyası kaybolsa bile ajanın config'indeki FAILOVER_HOME onu eve döndürür."""

    class MarkerFake(Fake):
        def call(self, method, path, body=None):
            if path.startswith("/api/config/resolved"):
                return 200, {"configs": [{"key": "FAILOVER_HOME", "value": "claude|claude-sonnet-5-5|medium"},
                                         {"key": "MODEL_OVERRIDE", "value": "gpt-6.1-sol"}]}
            return super().call(method, path, body)

    c = MarkerFake()
    state = {"agents": {}, "requeued": {}, "last_move": {}, "requeue_times": []}
    swapped = {**AG, "harnessProvider": "codex"}
    adopted = fo.adopt_home_markers(c, state, [swapped], ["codex"])
    assert adopted == ["Worker One (home=claude)"], adopted
    assert state["agents"]["a1"]["primary_provider"] == "claude"
    # limit açık → marka rağmen eve döner
    acts = fo.plan_routing(FAKE_ARGS, c, state, pstate(), [swapped])
    assert any(x.startswith("HOME") for x in acts), acts


def t_requeue_needs_child_dead_on_quota_only():
    c = Fake()
    state = {
        "agents": {}, "last_move": {}, "requeue_times": [],
        "requeued": {"t1": {"at": (NOW - timedelta(hours=2)).isoformat(), "to": "a9",
                            "new": "c1", "attempts": 1}},
    }
    child = {"id": "c1", "status": "failed", "parentTaskId": "t1", "createdAt": NOW.isoformat(),
             "failureReason": "Session error (exit code 1): pytest failed: 2 tests"}
    acts = fo.plan_recovery(FAKE_ARGS, c, state, pstate(), [FAILED_TASK, child], [{**AG, "id": "a9"}])
    assert acts == [], acts


def t_no_requeue_without_healthy_provider():
    c = Fake()
    state = {"agents": {}, "requeued": {}, "last_move": {}, "requeue_times": []}
    acts = fo.plan_recovery(
        FAKE_ARGS, c, state,
        pstate(claude_blocked_until=NOW + timedelta(hours=5), codex_blocked_until=NOW + timedelta(hours=5)),
        [FAILED_TASK], [{"id": "a9", "name": "W", "harnessProvider": "claude", "status": "idle"}],
    )
    assert acts == [], acts


def t_storm_marks_provider_blocked():
    tasks = [
        {"id": f"t{i}", "status": "failed", "provider": "claude",
         "createdAt": (NOW - timedelta(minutes=i + 1)).isoformat(),
         "failureReason": "Session error (exit code 1): You've hit your weekly limit · resets 8am (UTC)"}
        for i in range(3)
    ]
    p = pstate()  # claude görünürde sağlıklı
    storms = fo.apply_quota_storm(p, tasks, threshold=3)
    assert storms == {"claude": 3}, storms
    assert p["claude"]["blocked"] is True
    # blokli sayıldığı için taşıma yapılır, eve dönüş yapılmaz
    c = Fake()
    state = {"agents": {}, "requeued": {}, "last_move": {}, "requeue_times": []}
    fo.plan_routing(FAKE_ARGS, c, state, p, [AG])
    assert len(c.switched()) == 1, c.switched()


def t_hold_keeps_agent_on_alternate():
    c = Fake()
    state = {
        "agents": {"a1": {"primary_provider": "claude", "primary_model": "claude-sonnet-5-5",
                          "primary_effort": "medium", "hold": True}},
        "requeued": {}, "last_move": {}, "requeue_times": [],
    }
    swapped = {**AG, "harnessProvider": "codex"}
    fo.plan_routing(FAKE_ARGS, c, state, pstate(), [swapped])
    assert c.switched() == [], c.switched()


TESTS = [
    ("bloklu provider'daki bos ajan codex'e tasinir (+model/effort yazilir)", t_swap_when_home_blocked),
    ("ev saglikliyken tasima yapilmaz", t_no_swap_when_home_healthy),
    ("kapasite siniri (2) ve HOLD", t_capacity_holds),
    ("mesgul ajanin altindan provider degistirilmez", t_busy_agent_untouched),
    ("taze yanmis alternatife yuk alinmaz", t_burned_alternate_skipped),
    ("limit acilinca ajan eski provider/model/effort'a doner", t_home_when_limit_clears),
    ("dwell: ayni ajana 15 dk icinde ikinci dokunus yok", t_dwell_blocks_flapping),
    ("kotadan olen gorev reroute_fault ile yeniden gonderilir", t_requeue_quota_failure),
    ("gercek hatayla olen gorev tekrar gonderilmez", t_requeue_skips_real_failure),
    ("kurtarma denemesi kotadan olduyse tekrar denenir (attempt 2)", t_requeue_retries_when_child_died_on_quota),
    ("3 denemeden sonra durur", t_requeue_stops_after_max_attempts),
    ("state kaybolsa da FAILOVER_HOME markası ajanı eve döndürür", t_adopts_home_marker_when_state_lost),
    ("kopya gerçek hatayla öldüyse tekrar yok", t_requeue_needs_child_dead_on_quota_only),
    ("sağlıklı provider yokken kurtarma yok", t_no_requeue_without_healthy_provider),
    ("kota fırtınası: anahtarlar 'available' olsa da provider blokli sayılır", t_storm_marks_provider_blocked),
    ("hold: elle pinlenen ajan eve döndürülmez", t_hold_keeps_agent_on_alternate),
]

if __name__ == "__main__":
    ok = sum(run(name, fn) for name, fn in TESTS)
    print(f"\n{ok}/{len(TESTS)} test gecti")
    sys.exit(0 if ok == len(TESTS) else 1)
