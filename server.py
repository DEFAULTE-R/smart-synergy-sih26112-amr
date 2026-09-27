from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from pathlib import Path
import json, threading, time, os
from simulation import FleetSim, benchmark, validation, fairness_test, SCENARIOS
from edge_runtime import MultiProcessEdgeDemo, SCENARIOS

ROOT = Path(__file__).parent
sim = None
edge_demo = None
lock = threading.Lock()
running = True


def loop():
    while running:
        with lock:
            sim.step()
        time.sleep(0.20)



class Handler(BaseHTTPRequestHandler):
    def send_json(self, obj, filename=None):
        data = json.dumps(obj, indent=2).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        if filename:
            self.send_header('Content-Disposition', f'attachment; filename="{filename}"')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)
        if path == '/api/state':
            with lock:
                self.send_json(sim.state())
        elif path == '/api/reset':
            mode = qs.get('mode', ['distributed'])[0]
            scenario = qs.get('scenario', ['normal'])[0]
            if mode not in ('distributed', 'stopwait'):
                mode = 'distributed'
            if scenario not in SCENARIOS:
                scenario = 'normal'
            with lock:
                sim.mode = mode
                sim.scenario = scenario
                sim.reset()
                self.send_json({'ok': True, 'mode': mode, 'scenario': scenario})
        elif path == '/api/benchmark':
            scenarios = qs.get('scenarios', ['normal,blockage,failure,comm_loss,stress'])[0].split(',')
            scenarios = [s for s in scenarios if s in SCENARIOS]
            self.send_json(benchmark(scenarios=scenarios or None))
        elif path == '/api/validation':
            trials = int(qs.get('trials', ['10'])[0])
            self.send_json(validation(trials=trials))
        elif path == '/api/validation_export':
            trials = int(qs.get('trials', ['10'])[0])
            result = validation(trials=trials)
            self.send_json(result, 'amr_fleet_validation_v0.9.json')
        elif path == '/api/fairness':
            trials = int(qs.get('trials', ['10'])[0])
            ticks = int(qs.get('ticks', ['3000'])[0])
            self.send_json(fairness_test(trials=trials, ticks=ticks))
        elif path == '/api/fairness_export':
            trials = int(qs.get('trials', ['10'])[0])
            ticks = int(qs.get('ticks', ['3000'])[0])
            self.send_json(fairness_test(trials=trials, ticks=ticks), 'amr_fleet_fairness_v0.9.json')
        elif path == '/api/fail':
            robot = qs.get('robot', ['AMR-2'])[0]
            with lock:
                ok = sim.fail_robot(robot)
                self.send_json({'ok': ok, 'robot': robot})
        elif path == '/api/network':
            ticks = int(qs.get('ticks', ['20'])[0])
            drop = float(qs.get('drop', ['1.0'])[0])
            with lock:
                sim.set_network_loss(ticks, drop)
                self.send_json({'ok': True, 'ticks': ticks, 'drop_rate': drop})
        elif path == '/api/export':
            with lock:
                self.send_json(sim.state(), 'amr_fleet_run.json')
        elif path == '/api/edge/config':
            scenario = qs.get('scenario', ['normal'])[0]
            drop_raw = qs.get('drop', [None])[0]
            blockage_tick = int(qs.get('blockage_tick', ['10'])[0])
            with lock:
                try:
                    drop = None if drop_raw is None else float(drop_raw)
                    result = edge_demo.configure(scenario, drop, blockage_tick)
                except (ValueError, RuntimeError) as exc:
                    result = {'ok': False, 'error': str(exc)}
                self.send_json(result)
        elif path == '/api/edge/start':
            with lock:
                edge_demo.start()
                self.send_json(edge_demo.state())
        elif path == '/api/edge/state':
            with lock:
                self.send_json(edge_demo.state())
        elif path == '/api/edge/run':
            seconds = max(3, min(int(qs.get('seconds', ['30'])[0]), 120))
            with lock:
                self.send_json(edge_demo.run_for(seconds))
        elif path == '/api/edge/fail':
            robot = qs.get('robot', ['AMR-2'])[0]
            with lock:
                self.send_json(edge_demo.inject_failure(robot))
        elif path == '/api/edge/stop':
            with lock:
                edge_demo.stop()
                self.send_json({'ok': True, 'state': edge_demo.state()})
        elif path == '/':
            self.send_file(ROOT / 'static' / 'index.html', 'text/html')
        elif path == '/app.js':
            self.send_file(ROOT / 'static' / 'app.js', 'application/javascript')
        elif path == '/style.css':
            self.send_file(ROOT / 'static' / 'style.css', 'text/css')
        else:
            self.send_error(404)

    def send_file(self, p, ctype):
        data = p.read_bytes()
        self.send_response(200)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


def main():
    global sim, edge_demo
    sim = FleetSim(mode='distributed', scenario='normal', seed=26112)
    edge_demo = MultiProcessEdgeDemo()
    threading.Thread(target=loop, daemon=True).start()
    port = int(os.environ.get('PORT', '8007'))
    print(f'AMR Fleet SIH26112 running on port {port}')
    ThreadingHTTPServer(('0.0.0.0', port), Handler).serve_forever()


if __name__ == '__main__':
    main()
