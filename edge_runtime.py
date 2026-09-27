import json
import multiprocessing as mp
import queue
import random
import socket
import time
from dataclasses import dataclass, field

from simulation import WIDTH, HEIGHT, BASE_BLOCKS, BENCHMARK_TASKS, astar

TICK = 0.08
PEER_TTL = 8
ROBOT_IDS = ("AMR-1", "AMR-2", "AMR-3")
STARTS = {"AMR-1": (2, 2), "AMR-2": (22, 11), "AMR-3": (2, 11)}
PRIORITY = {"AMR-1": 1, "AMR-2": 2, "AMR-3": 3}
SCENARIOS = ("normal", "blockage", "comm_loss", "stress", "failure")
DEFAULT_DROP = {"normal": 0.0, "blockage": 0.0, "comm_loss": 0.35, "stress": 0.25, "failure": 0.0}
DYNAMIC_BLOCK = (12, 7)


@dataclass
class EdgeTask:
    id: str
    pickup: tuple
    drop: tuple
    stage: str = "TO_PICKUP"


@dataclass
class EdgeState:
    rid: str
    pos: tuple
    battery: float
    priority: int
    tasks: list = field(default_factory=list)
    task_index: int = 0
    path: list = field(default_factory=list)
    path_index: int = 0
    peer_states: dict = field(default_factory=dict)
    fairness_credit: float = 0.0
    completed: int = 0
    conflicts: int = 0
    stale_holds: int = 0
    packets_sent: int = 0
    packets_received: int = 0
    packets_dropped: int = 0
    blockage_replans: int = 0
    last_move_tick: int = 0


def neighbors(p):
    x, y = p
    for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1)):
        q = (x + dx, y + dy)
        if 0 <= q[0] < WIDTH and 0 <= q[1] < HEIGHT:
            yield q


def worker_main(rid, bind_addr, peer_addrs, task_payloads, report_q, stop_event,
                scenario="normal", drop_rate=0.0, blockage_tick=10):
    """One independently scheduled AMR process.

    The worker owns its socket, peer table, route and conflict decisions.
    The parent process supplies only startup configuration and receives reports.
    Scenario faults are local to this worker; they are not used to arbitrate motion.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(bind_addr)
    sock.setblocking(False)

    rng = random.Random(26112 + PRIORITY[rid] * 1009)
    effective_drop = max(0.0, min(1.0, float(drop_rate)))
    if scenario in ("comm_loss", "stress") and effective_drop <= 0:
        effective_drop = DEFAULT_DROP[scenario]

    tasks = [EdgeTask(t["id"], tuple(t["pickup"]), tuple(t["drop"]), t.get("stage", "TO_PICKUP"))
             for t in task_payloads]
    st = EdgeState(rid, STARTS[rid], 100.0 - (PRIORITY[rid] - 1) * 8,
                   PRIORITY[rid], tasks=tasks)
    blocks = set(BASE_BLOCKS)
    blockage_active = False
    tick = 0
    start_time = time.monotonic()

    def target():
        if st.task_index >= len(st.tasks):
            return None
        task = st.tasks[st.task_index]
        return task.pickup if task.stage == "TO_PICKUP" else task.drop

    def next_cell():
        if st.path_index + 1 < len(st.path):
            return st.path[st.path_index + 1]
        return st.pos

    def plan(goal, avoid=None, reason=""):
        if goal is None:
            return False
        blocked = set(blocks)
        if avoid:
            blocked.update(avoid)
        path = astar(st.pos, goal, blocked)
        if path:
            st.path, st.path_index = path, 0
            if reason == "dynamic blockage":
                st.blockage_replans += 1
            return True
        return False

    def score_local():
        return st.priority - 0.5 * st.fairness_credit

    def score_peer(p):
        return p.get("priority", 99) - 0.5 * p.get("fairness_credit", 0)

    def receive():
        while True:
            try:
                raw, _ = sock.recvfrom(8192)
            except BlockingIOError:
                return
            except OSError:
                return
            try:
                p = json.loads(raw.decode("utf-8"))
                if p.get("id") != rid:
                    st.peer_states[p["id"]] = p
                    st.packets_received += 1
            except (ValueError, KeyError, UnicodeDecodeError):
                pass

    def broadcast():
        packet = {
            "id": rid,
            "pos": list(st.pos),
            "intent": list(next_cell()),
            "priority": st.priority,
            "fairness_credit": round(st.fairness_credit, 3),
            "tick": tick,
            "ts": time.time(),
        }
        payload = json.dumps(packet, separators=(",", ":")).encode()
        for _, addr in peer_addrs.items():
            if effective_drop > 0 and rng.random() < effective_drop:
                st.packets_dropped += 1
                continue
            try:
                sock.sendto(payload, addr)
                st.packets_sent += 1
            except OSError:
                pass

    def report(event=""):
        report_q.put({
            "id": rid,
            "x": st.pos[0], "y": st.pos[1],
            "battery": round(st.battery, 2),
            "completed": st.completed,
            "task": st.tasks[st.task_index].id if st.task_index < len(st.tasks) else None,
            "stage": st.tasks[st.task_index].stage if st.task_index < len(st.tasks) else "DONE",
            "conflicts": st.conflicts,
            "stale_holds": st.stale_holds,
            "packets_sent": st.packets_sent,
            "packets_received": st.packets_received,
            "packets_dropped": st.packets_dropped,
            "blockage_replans": st.blockage_replans,
            "fairness_credit": round(st.fairness_credit, 2),
            "event": event,
            "tick": tick,
            "elapsed": round(time.monotonic() - start_time, 2),
        })

    report(f"PROCESS ONLINE; scenario={scenario}; drop_rate={effective_drop:.2f}")

    while not stop_event.is_set():
        tick += 1
        receive()

        if scenario in ("blockage", "stress") and not blockage_active and tick >= blockage_tick:
            blockage_active = True
            blocks.add(DYNAMIC_BLOCK)
            st.path = []
            st.path_index = 0
            report(f"DYNAMIC BLOCKAGE ACTIVE at {DYNAMIC_BLOCK}")

        broadcast()
        receive()

        if st.task_index >= len(st.tasks):
            report()
            time.sleep(TICK)
            continue

        goal = target()
        if not st.path or st.path_index >= len(st.path) - 1:
            plan(goal, reason="dynamic blockage" if blockage_active else "")

        desired = next_cell()
        visible = []
        for pid, p in list(st.peer_states.items()):
            age = tick - int(p.get("tick", tick))
            if age <= PEER_TTL:
                if tuple(p.get("intent", p.get("pos", st.pos))) == desired or tuple(p.get("pos", st.pos)) == desired:
                    visible.append(p)
                if tuple(p.get("pos", st.pos)) == desired and tuple(p.get("intent", st.pos)) == st.pos:
                    visible.append(p)
            else:
                last = tuple(p.get("pos", st.pos))
                if abs(last[0] - st.pos[0]) + abs(last[1] - st.pos[1]) <= 2 and desired in neighbors(st.pos):
                    st.stale_holds += 1
                    st.fairness_credit = min(8.0, st.fairness_credit + 0.5)
                    report(f"STALE PEER {pid}; safety hold")
                    time.sleep(TICK)
                    continue

        local_score = score_local()
        winner = True
        if visible:
            for p in visible:
                ps = score_peer(p)
                if ps < local_score or (ps == local_score and p["id"] < rid):
                    winner = False
                    break

        if winner and desired != st.pos:
            occupied = any(tuple(p.get("pos", ())) == desired for p in visible)
            if not occupied:
                st.pos = desired
                st.path_index += 1
                st.last_move_tick = tick
                st.battery = max(0, st.battery - 0.02)
                st.fairness_credit = max(0.0, st.fairness_credit - 0.25)
            else:
                winner = False

        if not winner:
            st.conflicts += 1
            st.fairness_credit = min(8.0, st.fairness_credit + 1.0)
            if st.fairness_credit >= 3 and desired != goal:
                plan(goal, avoid={desired}, reason="dynamic blockage" if blockage_active else "")

        if blockage_active and st.path and DYNAMIC_BLOCK in st.path[st.path_index:]:
            plan(goal, avoid={DYNAMIC_BLOCK}, reason="dynamic blockage")

        if st.pos == goal:
            task = st.tasks[st.task_index]
            if task.stage == "TO_PICKUP":
                task.stage = "TO_DROP"
                plan(task.drop, reason="dynamic blockage" if blockage_active else "")
            else:
                st.completed += 1
                st.task_index += 1
                if st.task_index < len(st.tasks):
                    plan(target(), reason="dynamic blockage" if blockage_active else "")
                else:
                    report(f"{task.id} completed")

        report()
        time.sleep(TICK)

    report("PROCESS STOPPED")
    sock.close()


class MultiProcessEdgeDemo:
    """Three actual OS processes + real UDP loopback transport.

    The parent is an observer/launcher and fault injector only; robot movement
    and conflict decisions happen inside the individual worker processes.
    """
    def __init__(self):
        self.ctx = mp.get_context("spawn")
        self.report_q = self.ctx.Queue()
        self.stop_event = self.ctx.Event()
        self.processes = []
        self.started = False
        self.addresses = {}
        self.latest = {}
        self.events = []
        self.started_at = None
        self.collision_evidence_count = 0
        self.collision_evidence_active = set()
        self.collision_evidence_ticks = {}
        self.scenario = "normal"
        self.drop_rate = 0.0
        self.blockage_tick = 10
        self.assignments = {}

    def _new_ports(self):
        chosen, sockets = [], []
        try:
            for _ in ROBOT_IDS:
                probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                probe.bind(("127.0.0.1", 0))
                chosen.append(probe.getsockname()[1])
                sockets.append(probe)
        finally:
            for probe in sockets:
                probe.close()
        return chosen

    def _assignments(self):
        return {
            "AMR-1": [BENCHMARK_TASKS[0], BENCHMARK_TASKS[3]],
            "AMR-2": [BENCHMARK_TASKS[1], BENCHMARK_TASKS[4]],
            "AMR-3": [BENCHMARK_TASKS[2], BENCHMARK_TASKS[5]],
        }

    def _spawn_worker(self, rid, task_payload):
        peers = {pid: self.addresses[pid] for pid in ROBOT_IDS if pid != rid}
        p = self.ctx.Process(
            target=worker_main,
            args=(rid, self.addresses[rid], peers, task_payload,
                  self.report_q, self.stop_event, self.scenario,
                  self.drop_rate, self.blockage_tick),
            daemon=True,
        )
        p.start()
        return p

    def configure(self, scenario="normal", drop_rate=None, blockage_tick=10):
        if self.started:
            raise RuntimeError("stop the edge demo before reconfiguring")
        if scenario not in SCENARIOS:
            scenario = "normal"
        self.scenario = scenario
        self.drop_rate = DEFAULT_DROP[scenario] if drop_rate is None else max(0.0, min(1.0, float(drop_rate)))
        self.blockage_tick = max(1, int(blockage_tick))
        return {
            "ok": True,
            "scenario": self.scenario,
            "drop_rate": self.drop_rate,
            "blockage_tick": self.blockage_tick,
        }

    def start(self):
        if self.started:
            return
        self.stop_event = self.ctx.Event()
        self.report_q = self.ctx.Queue()
        self.processes = []
        self.latest = {}
        # Each start is a fresh scenario run. Do not carry prior-run events
        # into the new scenario's event log.
        self.events = []
        self.collision_evidence_count = 0
        self.collision_evidence_active = set()
        self.collision_evidence_ticks = {}
        self.addresses = {}
        chosen = self._new_ports()
        for rid, port in zip(ROBOT_IDS, chosen):
            self.addresses[rid] = ("127.0.0.1", port)

        assignments = self._assignments()
        self.assignments = {}
        for rid in ROBOT_IDS:
            payload = [{
                "id": f"{rid}-T-{j+1}",
                "pickup": list(t[0]),
                "drop": list(t[1]),
            } for j, t in enumerate(assignments[rid])]
            self.assignments[rid] = payload
            self.processes.append(self._spawn_worker(rid, payload))

        self.started = True
        self.started_at = time.time()
        self.events.append({
            "t": 0,
            "msg": f"3 independent AMR processes started; scenario={self.scenario}; UDP endpoints active",
        })

    def poll(self):
        if not self.started:
            return
        while True:
            try:
                item = self.report_q.get_nowait()
            except queue.Empty:
                break
            self.latest[item["id"]] = item
            if item.get("event"):
                self.events.append({"t": item.get("elapsed", 0), "msg": f'{item["id"]}: {item["event"]}'})

        current = set()
        for i, rid_a in enumerate(ROBOT_IDS):
            a = self.latest.get(rid_a)
            if not a or a.get("x") is None or a.get("y") is None:
                continue
            for rid_b in ROBOT_IDS[i + 1:]:
                b = self.latest.get(rid_b)
                if not b or b.get("x") is None or b.get("y") is None:
                    continue
                # Only count evidence when the two asynchronous reports are
                # close in simulation time. This reduces false positives from
                # comparing snapshots that may be several worker ticks apart.
                tick_gap = abs(int(a.get("tick", 0)) - int(b.get("tick", 0)))
                elapsed_gap = abs(float(a.get("elapsed", 0.0)) - float(b.get("elapsed", 0.0)))
                if (a["x"], a["y"]) == (b["x"], b["y"]) and tick_gap == 0 and elapsed_gap <= 0.10:
                    current.add(tuple(sorted((rid_a, rid_b))))
        # Require the same pair to remain co-located across two distinct
        # worker ticks before recording evidence. This keeps the parent-side
        # metric useful without pretending asynchronous reports are a ground-truth snapshot.
        for pair in current:
            a = self.latest.get(pair[0], {})
            b = self.latest.get(pair[1], {})
            tick_key = min(int(a.get("tick", 0)), int(b.get("tick", 0)))
            previous = self.collision_evidence_ticks.get(pair)
            if previous is None:
                self.collision_evidence_ticks[pair] = tick_key
            elif tick_key > previous:
                self.collision_evidence_count += 1
                self.events.append({
                    "t": round(time.time() - (self.started_at or time.time()), 2),
                    "msg": f"APPROX COLLISION EVIDENCE: {pair[0]} / {pair[1]} shared latest reported cell across consecutive ticks",
                })
                self.collision_evidence_ticks[pair] = tick_key
        for pair in set(self.collision_evidence_ticks) - current:
            self.collision_evidence_ticks.pop(pair, None)
        self.collision_evidence_active = current
        self.events = self.events[-80:]

    def state(self):
        self.poll()
        rows = [self.latest.get(rid, {
            "id": rid, "x": None, "y": None, "completed": 0,
            "task": None, "stage": "STARTING", "packets_sent": 0,
            "packets_received": 0, "packets_dropped": 0,
            "conflicts": 0, "stale_holds": 0, "blockage_replans": 0,
            "fairness_credit": 0,
        }) for rid in ROBOT_IDS]
        return {
            "version": "v0.9",
            "mode": "multi-process edge runtime",
            "processes": len(self.processes),
            "alive_processes": sum(p.is_alive() for p in self.processes),
            "transport": "UDP loopback",
            "scenario": self.scenario,
            "drop_rate": self.drop_rate,
            "blockage_tick": self.blockage_tick,
            "addresses": {k: list(v) for k, v in self.addresses.items()},
            "robots": rows,
            "events": self.events[-40:],
            "started_at": self.started_at,
            "collision_evidence": {
                "count": self.collision_evidence_count,
                "method": "parent-side comparison of latest asynchronous worker reports; approximate, not a simultaneous ground-truth snapshot",
            },
        }

    def inject_failure(self, rid):
        if rid not in ROBOT_IDS:
            return {"ok": False, "error": "unknown robot"}
        if not self.started:
            return {"ok": False, "error": "edge demo not running"}
        idx = ROBOT_IDS.index(rid)
        p = self.processes[idx]
        if p.is_alive():
            p.terminate()
            p.join(timeout=1)
            self.events.append({"t": round(time.time() - (self.started_at or time.time()), 2),
                                "msg": f"FAULT INJECTED: {rid} process terminated"})
        return {"ok": True, "robot": rid, "alive": p.is_alive()}

    def stop(self):
        if not self.started:
            return
        self.stop_event.set()
        for p in self.processes:
            p.join(timeout=2)
            if p.is_alive():
                p.terminate()
        self.processes = []
        self.started = False
        self.events.append({"t": round(time.time() - (self.started_at or time.time()), 2),
                            "msg": "Edge processes stopped"})

    def run_for(self, seconds=30):
        self.start()
        deadline = time.time() + seconds
        while time.time() < deadline:
            self.poll()
            time.sleep(0.05)
        self.poll()
        result = self.state()
        result["duration"] = seconds
        result["all_tasks_completed"] = all(
            self.latest.get(rid, {}).get("completed", 0) >= 2 for rid in ROBOT_IDS
        )
        result["total_packets_sent"] = sum(self.latest.get(rid, {}).get("packets_sent", 0) for rid in ROBOT_IDS)
        result["total_packets_received"] = sum(self.latest.get(rid, {}).get("packets_received", 0) for rid in ROBOT_IDS)
        result["total_packets_dropped"] = sum(self.latest.get(rid, {}).get("packets_dropped", 0) for rid in ROBOT_IDS)
        result["total_conflicts"] = sum(self.latest.get(rid, {}).get("conflicts", 0) for rid in ROBOT_IDS)
        result["total_stale_holds"] = sum(self.latest.get(rid, {}).get("stale_holds", 0) for rid in ROBOT_IDS)
        result["total_blockage_replans"] = sum(self.latest.get(rid, {}).get("blockage_replans", 0) for rid in ROBOT_IDS)
        result["approx_collision_evidence_count"] = self.collision_evidence_count
        return result
