# Smart Synergy — SIH26112 AMR System Integration Demo

Supporting software prototype for **Smart India Hackathon 2026 — SIH26112**:

**Design and Develop a Modular Autonomous Mobile Robot (AMR) Platform for Smart Warehouse Automation**

Team: **Smart Synergy**  
Team ID: **160864**

## Purpose

This repository provides a software-side demonstration supporting the physical AMR concept. It focuses on **multi-AMR coordination, peer-to-peer communication, task execution, safety holds, dynamic blockage handling, process-failure observation, and fleet-state visualization**.

The software is supporting evidence for the AMR system-integration layer; the primary SIH26112 design evidence remains the Autodesk Fusion CAD, Generative Design, Topology Optimization, Simulation/FEA, DFAM and physical prototype work shown in the presentation.

## Demonstrated capabilities

- Three independently scheduled AMR worker processes
- Per-AMR UDP communication endpoints
- JSON-serialized robot state and movement intent exchange
- Distributed peer-state based movement/conflict decisions
- Task pickup/drop execution in the warehouse grid simulation
- Dynamic blockage detection and replanning
- Communication-loss / packet-drop scenarios
- Process-failure injection and observability
- Safety holds for stale peer information
- Packet sent/received/dropped counters
- Fleet event logging and live dashboard state
- Reproducible benchmark, validation and fairness-test paths

## Architecture

```text
                 Fleet Dashboard / HTTP Server
                    launch + observe + test
                              |
             +----------------+----------------+
             |                |                |
          AMR-1            AMR-2            AMR-3
        worker process   worker process   worker process
             | UDP            | UDP            | UDP
             +----------------+----------------+
                       peer state
```

The current demonstration runs the three AMR processes on **one computer using loopback UDP endpoints**. This is a software simulation/demo of the distributed worker architecture, not evidence of three separate physical edge computers.

## Scenarios

- `normal` — normal peer-to-peer operation
- `comm_loss` — injected outbound packet loss
- `blockage` — dynamic obstacle followed by replanning
- `stress` — combined communication loss and blockage
- `failure` — process-level AMR failure injection and observation

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m py_compile simulation.py server.py edge_runtime.py
python3 server.py
```

Open:

```text
http://127.0.0.1:8007
```

## Edge-process API

Normal run:

```text
GET /api/edge/config?scenario=normal
GET /api/edge/start
GET /api/edge/run?seconds=30
GET /api/edge/state
GET /api/edge/stop
```

Communication-loss test:

```text
GET /api/edge/config?scenario=comm_loss&drop=0.35
GET /api/edge/run?seconds=30
```

Dynamic blockage test:

```text
GET /api/edge/config?scenario=blockage&blockage_tick=10
GET /api/edge/run?seconds=30
```

Combined stress test:

```text
GET /api/edge/config?scenario=stress&drop=0.25&blockage_tick=10
GET /api/edge/run?seconds=30
```

Process-failure injection:

```text
GET /api/edge/fail?robot=AMR-2
GET /api/edge/state
```

## Evidence scope

The multi-process runtime directly demonstrates communication, peer-state exchange, stale-peer safety holds, dynamic blockage/replanning, process-failure injection, packet accounting and fleet-state observation.

The benchmark/validation functions in `simulation.py` are a separate reproducible simulation path. They should be interpreted as simulation evidence rather than physical AMR test results.

## Relationship to SIH26112

The repository complements the SIH26112 hardware concept by demonstrating the **system-integration and fleet-coordination software layer** around a modular AMR platform. It does not replace the Fusion-based mechanical design, DFAM, Generative Design, Topology Optimization or FEA evidence.
