import heapq
import statistics
import math
import random
import time
import socket
import json
from dataclasses import dataclass, field
from collections import deque

WIDTH, HEIGHT = 24, 14
TICK = 0.20
MAX_WAIT_BEFORE_DETOUR = 3
PEER_TTL_TICKS = 5

BASE_BLOCKS = set()
for x in range(WIDTH):
    BASE_BLOCKS.add((x, 0)); BASE_BLOCKS.add((x, HEIGHT - 1))
for y in range(HEIGHT):
    BASE_BLOCKS.add((0, y)); BASE_BLOCKS.add((WIDTH - 1, y))
for x1, x2, y1, y2 in [(4, 6, 2, 10), (9, 11, 3, 11), (14, 16, 2, 10), (19, 21, 3, 11)]:
    for x in range(x1, x2 + 1):
        for y in range(y1, y2 + 1):
            BASE_BLOCKS.add((x, y))

DOCKS = [(2, 2), (2, 11), (22, 2), (22, 11), (7, 2), (17, 11)]
PICKUPS = [(3, 3), (8, 11), (12, 2), (18, 10), (22, 7), (3, 9)]
DROPS = [(8, 2), (12, 11), (18, 2), (22, 10), (3, 6), (17, 12)]

BENCHMARK_TASKS = [
    (PICKUPS[0], DROPS[0]),
    (PICKUPS[1], DROPS[1]),
    (PICKUPS[2], DROPS[2]),
    (PICKUPS[3], DROPS[3]),
    (PICKUPS[4], DROPS[4]),
    (PICKUPS[5], DROPS[5]),
]

DIRS = [(1, 0), (-1, 0), (0, 1), (0, -1)]
SCENARIOS = ('normal', 'blockage', 'failure', 'comm_loss', 'stress', 'sustained_load')
CHOKE_POINTS = {(7,1), (12,1), (13,1), (17,1), (18,7), (22,6)}
FAIRNESS_CREDIT_CAP = 8


def neighbors(p):
    x, y = p
    for dx, dy in DIRS:
        q = (x + dx, y + dy)
        if 0 <= q[0] < WIDTH and 0 <= q[1] < HEIGHT:
            yield q


def astar(start, goal, blocked):
    if start == goal:
        return [start]
    blocked = set(blocked)
    blocked.discard(start)
    blocked.discard(goal)

    def h(a):
        return abs(a[0] - goal[0]) + abs(a[1] - goal[1])

    pq = [(h(start), 0, start)]
    came = {}
    g = {start: 0}
    seen = set()
    while pq:
        _, cost, u = heapq.heappop(pq)
        if u in seen:
            continue
        seen.add(u)
        if u == goal:
            path = [u]
            while u in came:
                u = came[u]
                path.append(u)
            return path[::-1]
        for v in neighbors(u):
            if v in blocked:
                continue
            ng = cost + 1
            if ng < g.get(v, 10**9):
                g[v] = ng
                came[v] = u
                heapq.heappush(pq, (ng + h(v), ng, v))
    return []


@dataclass
class Task:
    id: str
    pickup: tuple
    drop: tuple
    assigned: str = None
    stage: str = 'WAITING'
    created: float = 0.0
    completed_at: float = None


@dataclass
class Robot:
    id: str
    pos: tuple
    battery: float
    priority: int
    task: Task = None
    path: list = field(default_factory=list)
    path_index: int = 0
    status: str = 'IDLE'
    conflicts: int = 0
    replans: int = 0
    messages: int = 0
    messages_dropped: int = 0
    peer_states: dict = field(default_factory=dict)
    wait_ticks: int = 0
    completed: int = 0
    distance: int = 0
    last_conflict: tuple = None
    fairness_credit: int = 0
    fairness_wins: int = 0
    stale_holds: int = 0
    last_move_tick: int = 0
    goal_wait_ticks: int = 0

    def next_cell(self):
        if self.path_index + 1 < len(self.path):
            return self.path[self.path_index + 1]
        return self.pos


class FleetSim:
    def __init__(self, mode='distributed', scenario='normal', seed=26112):
        self.mode = mode
        self.scenario = scenario if scenario in SCENARIOS else 'normal'
        self.seed = seed
        self._init_transport()
        self.reset()

    def _init_transport(self):
        self.transport_sockets = {}
        self.transport_addresses = {}
        self.transport_packets_sent = 0
        self.transport_packets_received = 0
        self.transport_packets_dropped = 0
        self.transport_enabled = True
        base_port = 0
        for rid in ['AMR-1', 'AMR-2', 'AMR-3']:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('127.0.0.1', base_port))
            sock.setblocking(False)
            self.transport_sockets[rid] = sock
            self.transport_addresses[rid] = sock.getsockname()

    def close_transport(self):
        for sock in getattr(self, 'transport_sockets', {}).values():
            try: sock.close()
            except OSError: pass
        self.transport_sockets = {}

    def __del__(self):
        try: self.close_transport()
        except Exception: pass

    def reset(self):
        random.seed(self.seed)
        self.t = 0.0
        self.tick_count = 0
        self.finished = []
        self.log = deque(maxlen=140)
        self.blocks = set(BASE_BLOCKS)
        self.dynamic_block = None
        self.block_timer = 0
        self.failed_robot = None
        self.collisions = 0
        self.deadlocks = 0
        self.total_wait_ticks = 0
        self.reassignments = 0
        self.safety_holds = 0
        self.stale_peer_holds = 0
        self.blockage_reassignments = 0
        self.network_loss_ticks = 0
        self.network_events = 0
        self.network_drop_rate = 0.0
        self.transport_packets_sent = 0
        self.transport_packets_received = 0
        self.transport_packets_dropped = 0
        self.tasks = []
        self.next_task = 1
        self.starts = [(2, 2), (22, 11), (2, 11)]
        self.robots = [Robot(f'AMR-{i+1}', self.starts[i], 100 - i * 8, i + 1) for i in range(3)]
        for pickup, drop in BENCHMARK_TASKS:
            self.spawn_task(pickup, drop)
        self.assign_tasks()
        self.logmsg('SYSTEM', f'Fleet initialized: {self.mode.upper()} · scenario={self.scenario}')

    def logmsg(self, src, msg):
        self.log.appendleft({'t': round(self.t, 1), 'src': src, 'msg': msg})

    def spawn_task(self, pickup=None, drop=None):
        if pickup is None:
            pickup = random.choice(PICKUPS)
        if drop is None:
            drop = random.choice(DROPS)
            while drop == pickup:
                drop = random.choice(DROPS)
        task = Task(f'T-{self.next_task}', pickup, drop, created=self.t)
        self.next_task += 1
        self.tasks.append(task)
        return task

    def assign_tasks(self):
        for r in self.robots:
            if self.failed_robot == r.id:
                continue
            if r.task is None or r.task.stage == 'DONE':
                candidates = [t for t in self.tasks if t.stage == 'WAITING' and t.assigned is None]
                if not candidates:
                    continue
                # Local nearest-task allocation, with a small penalty for a congested robot.
                t = min(candidates, key=lambda q: (
                    abs(r.pos[0] - q.pickup[0]) + abs(r.pos[1] - q.pickup[1]),
                    r.priority,
                ))
                t.assigned = r.id
                t.stage = 'TO_PICKUP'
                r.task = t
                r.status = 'TO_PICKUP'
                self.plan(r, t.pickup)
                self.logmsg(r.id, f'Allocated {t.id} locally -> pickup {t.pickup}')

    def plan(self, r, target, avoid=None, reason='route'):
        if self.failed_robot == r.id:
            return False
        blocked = set(self.blocks)
        if avoid:
            blocked.update(avoid)
        path = astar(r.pos, target, blocked)
        if path:
            r.path = path
            r.path_index = 0
            r.replans += 1
            if reason != 'route':
                self.logmsg(r.id, f'Local replan ({reason}) -> {target}, {len(path)-1} hops')
            return True
        r.path = []
        r.status = 'WAITING_FOR_ROUTE'
        return False

    def _network_available(self):
        return self.network_loss_ticks <= 0

    def broadcast(self, r):
        """Transmit serialized peer state over real UDP loopback sockets.

        Each AMR has its own UDP endpoint. Packet loss is implemented by dropping
        packets before send, so peer_states contains only information that actually
        crossed the transport boundary.
        """
        if self.failed_robot == r.id:
            return
        packet = {
            'id': r.id,
            'pos': list(r.pos),
            'intent': list(r.next_cell()),
            'task': r.task.id if r.task else None,
            'priority': r.priority,
            'fairness_credit': r.fairness_credit,
            'ts_tick': self.tick_count,
            'ts': round(self.t, 1),
        }
        payload = json.dumps(packet, separators=(',', ':')).encode('utf-8')
        sender = self.transport_sockets.get(r.id)
        if sender is None:
            return
        for peer in self.robots:
            if peer.id == r.id or self.failed_robot == peer.id:
                continue
            if not self._network_available() or random.random() < self.network_drop_rate:
                r.messages_dropped += 1
                self.transport_packets_dropped += 1
                continue
            try:
                sender.sendto(payload, self.transport_addresses[peer.id])
                r.messages += 1
                self.transport_packets_sent += 1
            except OSError:
                r.messages_dropped += 1
                self.transport_packets_dropped += 1

        # Drain each AMR endpoint after all sends in this decision epoch.
        for observer in self.robots:
            if self.failed_robot == observer.id:
                continue
            sock = self.transport_sockets.get(observer.id)
            if sock is None:
                continue
            while True:
                try:
                    raw, _addr = sock.recvfrom(8192)
                except BlockingIOError:
                    break
                except OSError:
                    break
                try:
                    received = json.loads(raw.decode('utf-8'))
                    peer_id = received.get('id')
                    if peer_id and peer_id != observer.id:
                        observer.peer_states[peer_id] = received
                        self.transport_packets_received += 1
                except (ValueError, UnicodeDecodeError):
                    self.transport_packets_dropped += 1
        return packet

    def _peer_fresh(self, observer, peer_id):
        packet = observer.peer_states.get(peer_id)
        return packet is not None and (self.tick_count - packet.get('ts_tick', self.tick_count)) <= PEER_TTL_TICKS

    def _peer_score(self, packet):
        return packet['priority'] - 0.5 * packet.get('fairness_credit', 0)

    def _local_score(self, robot):
        return robot.priority - 0.5 * robot.fairness_credit

    def _choose_local_winner(self, robot, packet):
        """Compare only information the robot has received from that peer."""
        local_score = self._local_score(robot)
        peer_score = self._peer_score(packet)
        if local_score < peer_score:
            return True
        if local_score > peer_score:
            return False
        return robot.id < packet['id']

    def _known_conflicts(self, r, target):
        """Return conflicts visible from r.peer_states only.

        This deliberately does not inspect another Robot object's live position or intent.
        The physical collision counter remains a global observer in step(), but it is not
        used here to make the coordination decision.
        """
        conflicts = []
        for peer_id, packet in r.peer_states.items():
            if peer_id == r.id or peer_id == self.failed_robot:
                continue
            if not self._peer_fresh(r, peer_id):
                continue
            peer_pos = tuple(packet['pos'])
            peer_intent = tuple(packet['intent'])
            if peer_intent == target or peer_pos == target or (peer_intent == r.pos and target == peer_pos):
                conflicts.append(packet)
        return conflicts

    def _stale_peer_near_target(self, r, target):
        """Use only the last packet received by r to decide whether to hold conservatively."""
        for peer_id, packet in r.peer_states.items():
            if peer_id == r.id or peer_id == self.failed_robot:
                continue
            if self._peer_fresh(r, peer_id):
                continue
            last_pos = tuple(packet['pos'])
            if target in CHOKE_POINTS or abs(last_pos[0] - target[0]) + abs(last_pos[1] - target[1]) <= 1:
                return packet
        return None

    def resolve_distributed(self, proposals):
        """Peer-state-driven coordination.

        Each robot decides from its own received peer table. No live peer Robot object is
        consulted for arbitration. A stale peer causes a conservative hold near choke points.
        """
        active = {r.id: (r, target) for r, target in proposals}
        decisions = {}

        for r, target in list(active.values()):
            stale = None if self._network_available() else self._stale_peer_near_target(r, target)
            if stale is not None:
                r.conflicts += 1
                r.wait_ticks += 1
                r.fairness_credit = min(FAIRNESS_CREDIT_CAP, r.fairness_credit + 1)
                r.stale_holds += 1
                self.stale_peer_holds += 1
                self.safety_holds += 1
                r.last_conflict = target
                self.logmsg(r.id, f'STALE PEER {stale["id"]}; conservative hold at {target}')
                decisions[r.id] = False
                continue

            visible = self._known_conflicts(r, target)
            if not visible:
                decisions[r.id] = True
                continue
            wins = all(self._choose_local_winner(r, p) for p in visible)
            if wins:
                decisions[r.id] = True
                r.fairness_wins += 1
            else:
                decisions[r.id] = False
                r.conflicts += 1
                r.wait_ticks += 1
                r.fairness_credit = min(FAIRNESS_CREDIT_CAP, r.fairness_credit + 1)
                r.last_conflict = target
                winner = min(visible, key=self._peer_score)
                self.logmsg(r.id, f'Peer negotiation: yielding to {winner["id"]} at {target} (credit={r.fairness_credit})')

                if r.wait_ticks >= MAX_WAIT_BEFORE_DETOUR:
                    task_target = r.task.pickup if r.task and r.task.stage == 'TO_PICKUP' else r.task.drop if r.task else r.pos
                    # Never force a detour through the actual task goal: A* must not be
                    # allowed to treat an occupied goal as avoidable. Hold until the peer
                    # clears it, then the next tick's peer exchange decides again.
                    if target == task_target:
                        r.goal_wait_ticks += 1
                        if r.goal_wait_ticks >= 15:
                            self._release_task_for_reassignment(r, reason='goal congestion')
                            decisions[r.id] = False
                            active.pop(r.id, None)
                            self.logmsg(r.id, f'Goal congestion exceeded fairness window at {target}; task reallocated')
                        else:
                            decisions[r.id] = False
                            self.logmsg(r.id, f'Goal cell occupied; holding for peer clearance at {target} (goal wait={r.goal_wait_ticks})')
                    else:
                        avoid = {target}
                        if self.plan(r, task_target, avoid=avoid, reason='fairness detour'):
                            active[r.id] = (r, r.next_cell())
                            decisions[r.id] = False
                            self.logmsg(r.id, f'Fairness detour planned after {r.wait_ticks} waits; re-evaluating next tick')

        # Remove losing proposals. A robot can only enter a cell when its own peer table
        # permits it; there is no global proposal-pair arbitration here.
        accepted = []
        for r, target in active.values():
            if decisions.get(r.id, False):
                accepted.append((r, target))
        return accepted

    def resolve_stopwait(self, proposals):
        if not proposals:
            return []
        ordered = sorted(proposals, key=lambda x: (x[0].priority, x[0].id))
        occupied = {r.pos for r in self.robots if self.failed_robot != r.id}
        for r, target in ordered:
            if target in occupied:
                r.conflicts += 1
                r.wait_ticks += 1
                continue
            r.wait_ticks = 0
            return [(r, target)]
        for r, _ in ordered:
            r.conflicts += 1
            r.wait_ticks += 1
        return []

    def _release_task_for_reassignment(self, robot, reason='reassignment'):
        if not robot.task:
            return False
        task = robot.task
        task.assigned = None
        task.stage = 'WAITING'
        robot.task = None
        robot.path = []
        robot.status = 'IDLE'
        self.reassignments += 1
        if reason.startswith('blocked aisle'):
            self.blockage_reassignments += 1
        self.logmsg('SYSTEM', f'{task.id} released from {robot.id} -> fleet reassignment ({reason})')
        self.assign_tasks()
        return True

    def trigger_blockage(self):
        candidates = [(7, 1), (12, 1), (13, 1), (17, 1), (18, 7), (22, 6)]
        route_cells = {cell for r in self.robots if r.path for cell in r.path[r.path_index:]}
        preferred = [c for c in candidates if c in route_cells and c not in self.blocks]
        free = preferred or [c for c in candidates if c not in self.blocks]
        if not free:
            return False
        self.dynamic_block = free[0]
        self.blocks.add(self.dynamic_block)
        self.block_timer = 45
        self.logmsg('SYSTEM', f'DYNAMIC BLOCKAGE at {self.dynamic_block}; affected AMRs rerouting')
        affected = []
        for r in self.robots:
            if r.path and self.dynamic_block in r.path[r.path_index:]:
                affected.append(r)
                target = r.task.pickup if r.task and r.task.stage == 'TO_PICKUP' else r.task.drop if r.task else r.pos
                ok = self.plan(r, target, avoid={self.dynamic_block}, reason='dynamic blockage')
                if not ok and r.task:
                    self._release_task_for_reassignment(r, reason='blocked aisle')

        # If an affected AMR is forced into a long detour, hand its task back to the
        # fleet instead of making it own the blocked aisle indefinitely. This is the
        # explicit blockage -> task-reassignment path required by the PS.
        for r in affected:
            if r.task and r.task.stage in ('TO_PICKUP', 'TO_DROP'):
                target = r.task.pickup if r.task.stage == 'TO_PICKUP' else r.task.drop
                remaining = astar(r.pos, target, self.blocks)
                if not remaining or len(remaining) - 1 > 18:
                    self._release_task_for_reassignment(r, reason='blocked aisle reroute cost')
                    break
        return True

    def maybe_dynamic_block(self):
        if self.block_timer > 0:
            self.block_timer -= 1
            if self.block_timer == 0 and self.dynamic_block:
                self.blocks.discard(self.dynamic_block)
                self.logmsg('SYSTEM', f'Obstacle cleared at {self.dynamic_block}')
                self.dynamic_block = None
            return
        if self.scenario in ('blockage', 'stress') and self.tick_count == 10:
            self.trigger_blockage()

    def set_network_loss(self, ticks=20, drop_rate=1.0):
        self.network_loss_ticks = max(0, int(ticks))
        self.network_drop_rate = max(0.0, min(1.0, float(drop_rate)))
        self.network_events += 1
        self.logmsg('NETWORK', f'P2P degradation injected for {self.network_loss_ticks} ticks · drop={round(self.network_drop_rate*100)}%')

    def maybe_network_event(self):
        if self.network_loss_ticks > 0:
            self.network_loss_ticks -= 1
            if self.network_loss_ticks == 0:
                self.network_drop_rate = 0.0
                self.logmsg('NETWORK', 'P2P communication restored')
            return
        if self.scenario in ('comm_loss', 'stress') and self.tick_count == 8:
            self.set_network_loss(22, 1.0)

    def fail_robot(self, robot_id):
        if robot_id not in {r.id for r in self.robots} or self.failed_robot:
            return False
        self.failed_robot = robot_id
        failed = next(r for r in self.robots if r.id == robot_id)
        failed.status = 'FAILED'
        self.logmsg('SYSTEM', f'{robot_id} failure injected; task recovery started')
        if failed.task:
            task = failed.task
            task.assigned = None
            task.stage = 'WAITING'
            failed.task = None
            failed.path = []
            self.reassignments += 1
            self.logmsg('SYSTEM', f'{task.id} released from {robot_id} -> fleet reassignment')
        self.assign_tasks()
        return True

    def maybe_sustained_load(self):
        if self.scenario != 'sustained_load':
            return
        # Keep a rolling workload so fairness is exercised under continuous contention.
        if self.tick_count > 1 and self.tick_count % 18 == 0 and len([t for t in self.tasks if t.stage == 'WAITING']) < 2:
            self.spawn_task()
            self.logmsg('SYSTEM', 'Sustained workload: new task injected')

    def maybe_failure_event(self):
        if self.scenario in ('failure', 'stress') and self.tick_count == 12:
            self.fail_robot('AMR-2')

    def step(self):
        self.t += TICK
        self.tick_count += 1
        self.maybe_dynamic_block()
        self.maybe_network_event()
        self.maybe_failure_event()
        self.maybe_sustained_load()
        self.assign_tasks()

        proposals = []
        for r in self.robots:
            if self.failed_robot == r.id:
                continue
            if not r.task:
                r.status = 'IDLE'
                continue
            if r.task.stage == 'TO_PICKUP' and r.pos == r.task.pickup:
                r.task.stage = 'TO_DROP'
                r.status = 'TO_DROP'
                self.plan(r, r.task.drop, reason='pickup reached')
                self.logmsg(r.id, f'{r.task.id}: pickup reached -> destination {r.task.drop}')
            elif r.task.stage == 'TO_DROP' and r.pos == r.task.drop:
                r.task.stage = 'DONE'
                r.task.completed_at = self.t
                r.completed += 1
                self.finished.append(r.task.id)
                self.logmsg(r.id, f'{r.task.id}: COMPLETED')
                r.task = None
                r.path = []
                r.status = 'IDLE'
                r.battery = max(0, r.battery - 3)
                continue
            if r.task and r.path:
                target = r.next_cell()
                if target != r.pos:
                    proposals.append((r, target))

        # Broadcast the current intent after task-stage transitions/replanning so peers
        # arbitrate on the same decision epoch. Packet loss can still hide this update.
        for r in self.robots:
            self.broadcast(r)

        accepted = self.resolve_stopwait(proposals) if self.mode == 'stopwait' else self.resolve_distributed(proposals)
        occupied = {r.pos: r.id for r in self.robots if self.failed_robot != r.id}
        for r, target in accepted:
            if target in occupied and occupied[target] != r.id:
                self.safety_holds += 1
                continue
            r.pos = target
            r.path_index += 1
            r.wait_ticks = 0
            r.goal_wait_ticks = 0
            r.fairness_credit = max(0, r.fairness_credit - 1)
            r.last_move_tick = self.tick_count
            r.distance += 1
            r.battery = max(0, r.battery - 0.015)

        positions = [r.pos for r in self.robots if self.failed_robot != r.id]
        if len(set(positions)) < len(positions):
            self.collisions += 1
            self.logmsg('SAFETY', 'Collision guard triggered')

        for r in self.robots:
            if self.failed_robot == r.id or not r.task:
                continue
            if self.dynamic_block and r.wait_ticks >= 8:
                self._release_task_for_reassignment(r, reason='blocked aisle delay')
                r.wait_ticks = 0
                continue
            if r.wait_ticks >= 10:
                self.deadlocks += 1
                target = r.task.pickup if r.task.stage == 'TO_PICKUP' else r.task.drop
                avoid = {r.last_conflict} if r.last_conflict else set()
                self.plan(r, target, avoid=avoid, reason='deadlock recovery')
                r.wait_ticks = 0
                self.logmsg(r.id, f'Deadlock recovery triggered -> {target}')
            self.total_wait_ticks += r.wait_ticks

        if self.tick_count % 20 == 0:
            self.assign_tasks()

    def done(self):
        return len(self.finished) == len(self.tasks)

    def state(self):
        return {
            'time': round(self.t, 1),
            'tick': self.tick_count,
            'mode': self.mode,
            'scenario': self.scenario,
            'collisions': self.collisions,
            'completed': len(self.finished),
            'tasks_total': len(self.tasks),
            'dynamic_block': self.dynamic_block,
            'deadlocks': self.deadlocks,
            'reassignments': self.reassignments,
            'waiting': self.total_wait_ticks,
            'safety_holds': self.safety_holds,
            'stale_peer_holds': self.stale_peer_holds,
            'blockage_reassignments': self.blockage_reassignments,
            'network_loss_ticks': self.network_loss_ticks,
            'network_events': self.network_events,
            'network_healthy': self._network_available(),
            'transport': {'type': 'UDP loopback', 'enabled': self.transport_enabled, 'packets_sent': self.transport_packets_sent, 'packets_received': self.transport_packets_received, 'packets_dropped': self.transport_packets_dropped, 'endpoints': {rid: list(addr) for rid, addr in self.transport_addresses.items()}},
            'robots': [{
                'id': r.id,
                'x': r.pos[0],
                'y': r.pos[1],
                'battery': round(r.battery, 1),
                'status': r.status,
                'task': r.task.id if r.task else None,
                'target': list((r.task.pickup if r.task and r.task.stage == 'TO_PICKUP' else r.task.drop) if r.task else r.pos),
                'route': [list(x) for x in r.path[r.path_index:]],
                'conflicts': r.conflicts,
                'replans': r.replans,
                'messages': r.messages,
                'messages_dropped': r.messages_dropped,
                'priority': r.priority,
                'completed': r.completed,
                'distance': r.distance,
                'fairness_credit': r.fairness_credit,
                'fairness_wins': r.fairness_wins,
                'stale_holds': r.stale_holds,
                'goal_wait_ticks': r.goal_wait_ticks,
            } for r in self.robots],
            'tasks': [{
                'id': t.id,
                'pickup': list(t.pickup),
                'drop': list(t.drop),
                'assigned': t.assigned,
                'stage': t.stage,
                'completed_at': t.completed_at,
            } for t in self.tasks],
            'blocks': [list(x) for x in self.blocks],
            'log': list(self.log),
        }


def run_experiment(mode, max_ticks=2500, dynamic_events=False, seed=26112, scenario='normal'):
    if dynamic_events and scenario == 'normal':
        scenario = 'blockage'
    sim = FleetSim(mode=mode, scenario=scenario, seed=seed)
    started = time.perf_counter()
    if scenario == 'sustained_load':
        while sim.tick_count < max_ticks:
            sim.step()
    else:
        while not sim.done() and sim.tick_count < max_ticks:
            sim.step()
    elapsed = time.perf_counter() - started
    total_messages = sum(r.messages for r in sim.robots)
    dropped = sum(r.messages_dropped for r in sim.robots)
    result = {
        'mode': mode,
        'scenario': scenario,
        'completed': len(sim.finished),
        'tasks_total': len(sim.tasks),
        'sim_time': round(sim.t, 1),
        'ticks': sim.tick_count,
        'collisions': sim.collisions,
        'deadlocks': sim.deadlocks,
        'reassignments': sim.reassignments,
        'waiting_ticks': sim.total_wait_ticks,
        'safety_holds': sim.safety_holds,
        'messages': total_messages,
        'messages_dropped': dropped,
        'replans': sum(r.replans for r in sim.robots),
        'distance': sum(r.distance for r in sim.robots),
        'wall_time': round(elapsed, 4),
        'completed_all': sim.done(),
        'transport': {
            'type': 'UDP loopback',
            'packets_sent': sim.transport_packets_sent,
            'packets_received': sim.transport_packets_received,
            'packets_dropped': sim.transport_packets_dropped,
        },
    }
    sim.close_transport()
    return result


def fairness_test(trials=10, ticks=3000, base_seed=26112):
    """Continuous-load fairness test for starvation, using the sustained workload scenario."""
    trials = max(1, min(int(trials), 30))
    rows = []
    for i in range(trials):
        seed = base_seed + i
        sim = FleetSim(mode='distributed', scenario='sustained_load', seed=seed)
        for _ in range(ticks):
            sim.step()
        counts = [r.completed for r in sim.robots]
        waits = [r.wait_ticks for r in sim.robots]
        total = sum(counts)
        jain = (total * total) / (len(counts) * sum(c * c for c in counts)) if total else 0.0
        rows.append({
            'seed': seed,
            'ticks': ticks,
            'completed_total': total,
            'completed_by_robot': dict(zip([r.id for r in sim.robots], counts)),
            'max_wait_ticks': max(waits) if waits else 0,
            'fairness_index': round(jain, 4),
            'deadlocks': sim.deadlocks,
            'collisions': sim.collisions,
            'transport_packets_sent': sim.transport_packets_sent,
            'transport_packets_received': sim.transport_packets_received,
        })
        sim.close_transport()
    return {
        'version': 'v0.6',
        'trials': trials,
        'ticks': ticks,
        'rows': rows,
        'mean_completed': round(statistics.mean(r['completed_total'] for r in rows), 3),
        'mean_fairness_index': round(statistics.mean(r['fairness_index'] for r in rows), 4),
        'min_fairness_index': round(min(r['fairness_index'] for r in rows), 4),
        'max_wait_ticks': max(r['max_wait_ticks'] for r in rows),
        'collision_free_trials': sum(r['collisions'] == 0 for r in rows),
    }

def benchmark(max_ticks=2500, seed=26112, scenarios=None):
    scenarios = scenarios or ['normal', 'blockage', 'failure', 'comm_loss', 'stress']
    results = []
    for scenario in scenarios:
        baseline = run_experiment('stopwait', max_ticks=max_ticks, seed=seed, scenario=scenario)
        distributed = run_experiment('distributed', max_ticks=max_ticks, seed=seed, scenario=scenario)
        improvement = None
        if baseline['completed_all'] and distributed['completed_all'] and baseline['sim_time'] > 0:
            improvement = round((baseline['sim_time'] - distributed['sim_time']) / baseline['sim_time'] * 100, 2)
        results.append({'scenario': scenario, 'baseline': baseline, 'distributed': distributed, 'improvement_pct': improvement})
    normal = next((x for x in results if x['scenario'] == 'normal'), None)
    return {
        'scenarios': results,
        'baseline': normal['baseline'] if normal else results[0]['baseline'],
        'distributed': normal['distributed'] if normal else results[0]['distributed'],
        'improvement_pct': normal['improvement_pct'] if normal else None,
    }


def _stats(values):
    if not values:
        return {
            'n': 0, 'mean': None, 'median': None, 'std': None,
            'min': None, 'max': None, 'ci95_low': None, 'ci95_high': None
        }
    vals = [float(v) for v in values]
    mean = statistics.mean(vals)
    median = statistics.median(vals)
    std = statistics.stdev(vals) if len(vals) > 1 else 0.0
    margin = 1.96 * std / math.sqrt(len(vals)) if len(vals) > 1 else 0.0
    return {
        'n': len(vals),
        'mean': round(mean, 3),
        'median': round(median, 3),
        'std': round(std, 3),
        'min': round(min(vals), 3),
        'max': round(max(vals), 3),
        'ci95_low': round(mean - margin, 3),
        'ci95_high': round(mean + margin, 3),
    }


def validation(trials=10, base_seed=26112, max_ticks=2500, scenarios=None):
    """Paired, repeatable validation using identical seeds/workloads per mode."""
    trials = max(1, min(int(trials), 30))
    scenarios = scenarios or ['normal', 'blockage', 'failure', 'comm_loss', 'stress']
    seeds = [base_seed + i for i in range(trials)]
    rows = []

    for scenario in scenarios:
        paired = []
        for seed in seeds:
            baseline = run_experiment(
                'stopwait', max_ticks=max_ticks, seed=seed, scenario=scenario
            )
            distributed = run_experiment(
                'distributed', max_ticks=max_ticks, seed=seed, scenario=scenario
            )
            improvement = None
            if baseline['completed_all'] and distributed['completed_all'] and baseline['sim_time'] > 0:
                improvement = (baseline['sim_time'] - distributed['sim_time']) / baseline['sim_time'] * 100.0
            paired.append({
                'seed': seed,
                'distributed': distributed,
                'baseline': baseline,
                'improvement_pct': None if improvement is None else round(improvement, 3),
            })

        d_times = [x['distributed']['sim_time'] for x in paired if x['distributed']['completed_all']]
        b_times = [x['baseline']['sim_time'] for x in paired if x['baseline']['completed_all']]
        improvements = [x['improvement_pct'] for x in paired if x['improvement_pct'] is not None]
        d_completed = sum(x['distributed']['completed_all'] for x in paired)
        b_completed = sum(x['baseline']['completed_all'] for x in paired)
        d_collision_free = sum(x['distributed']['collisions'] == 0 for x in paired)
        b_collision_free = sum(x['baseline']['collisions'] == 0 for x in paired)

        rows.append({
            'scenario': scenario,
            'trials': trials,
            'seed_range': [seeds[0], seeds[-1]],
            'distributed': {
                'completion_rate_pct': round(d_completed / trials * 100, 2),
                'collision_free_rate_pct': round(d_collision_free / trials * 100, 2),
                'sim_time': _stats(d_times),
                'collisions_total': sum(x['distributed']['collisions'] for x in paired),
                'mean_replans': round(statistics.mean(x['distributed']['replans'] for x in paired), 3),
                'mean_messages': round(statistics.mean(x['distributed']['messages'] for x in paired), 3),
            },
            'stopwait': {
                'completion_rate_pct': round(b_completed / trials * 100, 2),
                'collision_free_rate_pct': round(b_collision_free / trials * 100, 2),
                'sim_time': _stats(b_times),
                'collisions_total': sum(x['baseline']['collisions'] for x in paired),
                'mean_replans': round(statistics.mean(x['baseline']['replans'] for x in paired), 3),
                'mean_messages': round(statistics.mean(x['baseline']['messages'] for x in paired), 3),
            },
            'improvement_pct': _stats(improvements),
            'paired_successes': len(improvements),
            'target_pct': 20.0,
            'raw': paired,
        })

    valid_improvements = [
        v for row in rows for v in
        [x['improvement_pct'] for x in row['raw'] if x['improvement_pct'] is not None]
    ]
    all_distributed_completed = all(
        x['distributed']['completion_rate_pct'] == 100 for x in rows
    )
    return {
        'version': 'v0.6',
        'method': 'paired multi-trial validation',
        'trials_per_scenario': trials,
        'seed_range': [seeds[0], seeds[-1]],
        'target_pct': 20.0,
        'scenarios': rows,
        'overall': {
            'successful_pairs': len(valid_improvements),
            'mean_improvement_pct': round(statistics.mean(valid_improvements), 3) if valid_improvements else None,
            'median_improvement_pct': round(statistics.median(valid_improvements), 3) if valid_improvements else None,
            'std_improvement_pct': round(statistics.stdev(valid_improvements), 3) if len(valid_improvements) > 1 else 0.0,
            'ci95_improvement': _stats(valid_improvements),
            'distributed_100pct_completion_all_scenarios': all_distributed_completed,
        },
    }
