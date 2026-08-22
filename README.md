# LastState Platform Fleet Simulator

Simulates a fleet of embedded devices running **Latch** firmware against a real
**Relay** gateway, so `docker compose up` turns on the entire LastState
platform end to end — and you can watch every layer work.

```
                ┌────────────────────────────────────────────────┐
                │            SIMULATED DEVICE FLEET              │
                │  stm32 · esp32 · nrf52 · riscv · linux-edge    │
                │  (LEP v2: identity, CPU/fault ctx, breadcrumbs)│
                └───────┬──────────┬───────────────┬─────────────┘
                        │ HTTP     │ TCP (COBS)    │ UDP
                        ▼          ▼               ▼
                ┌────────────────────────────────────────────────┐
                │  RELAY — offline-first gateway                 │
                │  persist-before-ACK · spool · retries · LSAK   │
                └───────────────────┬────────────────────────────┘
                                    │ batch, zstd, mirror delivery
                                    ▼
                ┌────────────────────────────────────────────────┐
                │  TRACE — backend + UI                          │
                │  grouping · symbolication · alerts · dashboards│
                └────────────────────────────────────────────────┘
```

## What it demonstrates

| Platform behavior | How the simulator shows it |
|---|---|
| **Protocol** | Encodes LEP v2 envelopes byte-exact (magic `LSTP`, header+payload CRC-32, TLV records for identity, CPU/fault registers, breadcrumbs, heap, metrics, resets) |
| **Every transport** | HTTP (single + binary batch), persistent TCP with COBS framing, fire-and-forget UDP — each device archetype is pinned to one |
| **LSAK acknowledgements** | TCP devices read `LSAK` frames from Relay and credit `stored`/`duplicate`/`nack-*` per event ID |
| **Offline-first** | During network outages devices park evidence in a bounded ring buffer (newest wins) and flush FIFO on reconnect — duplicates get deduped by event ID downstream |
| **Incident scenarios** | Scripted rotation: steady telemetry → crash storm → network outage → recovery flush → boot loop |
| **Observability** | Prometheus metrics on `:9468` (`laststate_sim_*`) and a fleet state table every 15s |

## Run the whole platform

From the repository root (where `docker-compose.yml` lives):

```bash
docker compose up --build
```

Then watch:

- **Simulator console** — the fleet table and per-event log lines (`⚡` delivered, `⧗` buffered offline, `↻` flush after reconnect, `✓ [lsak]` acknowledgements)
- **Trace UI** — http://localhost:8080 (crash storms group into issues; boot loops show rising boot counters)
- **Relay admin** — http://localhost:8383 (sources up, spool depth, delivery state)
- **Grafana** — http://localhost:3000 (relay + simulator metrics)
- **Simulator metrics** — http://localhost:9468/metrics

## Scenario timeline (demo mode)

| Phase | Duration | What to watch |
|---|---|---|
| `steady` | 60s | heartbeats/logs flowing Relay → Trace |
| `crash-storm` | 45s | hard faults, asserts, watchdogs; Trace groups them |
| `network-outage` | 40s | `⧗` buffered events pile up; Relay delivery retries |
| `steady` (recovery) | 30s | `↻` flush of parked evidence; dedup by event ID |
| `boot-loop` | 30s | reset cascade with rising boot counts |

Pin one scenario with `SIM_SCENARIO=steady|crash-storm|network-outage|boot-loop`.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `RELAY_URL` | `http://relay:8384` | Relay HTTP source |
| `RELAY_TOKEN` | `dev-ingest-token-12345` | Ingest bearer token |
| `SIM_TCP_ADDR` | `relay:8385` | Relay TCP source (COBS + LSAK) |
| `SIM_UDP_ADDR` | `relay:8386` | Relay UDP source |
| `SIMULATOR_RATE` | `4` | Events per second (fleet-wide) |
| `SIMULATOR_BATCH_SIZE` | `5` | Envelopes per HTTP batch burst |
| `SIMULATOR_DEVICE_COUNT` | `5` | Fleet size (archetypes repeat with unique IDs) |
| `SIMULATOR_ERROR_RATE` | `0.35` | Baseline fault probability (scenarios override) |
| `SIM_SCENARIO` | `demo` | `demo` rotates all phases; or pin one |
| `SIM_METRICS_LISTEN` | `:9468` | Prometheus listener |
| `SIM_BUFFER_CAP` | `256` | Offline ring size per device |

## Device archetypes

| Device | Arch | RTOS | Transport | Why |
|---|---|---|---|---|
| `stm32-meter-001` | Cortex-M4 | FreeRTOS | TCP | Metering node on a gateway backhaul — ACK path matters |
| `esp32-gateway-002` | Xtensa LX6 | ESP-IDF | HTTP | Wi-Fi hub, batches many events |
| `nrf52-beacon-003` | Cortex-M4F | Zephyr | UDP | Battery beacon, datagram uplink |
| `riscv-solar-004` | RV32IMAC | FreeRTOS | TCP | Solar controller on leased line |
| `linux-edge-node-005` | x86/ARM64 | Linux | HTTP | Edge aggregator with rich logs |

## Development

```bash
go test ./...        # unit tests incl. COBS round-trip and a TCP↔LSAK loop
go vet ./...
go build .
```

Internal layout: `internal/lep` (wire format), `internal/device` (state
machines + offline buffer), `internal/transport` (HTTP/TCP/UDP senders),
`internal/scenario` (phase engine), `internal/report` (console + metrics).

Proprietary — part of the LastState platform.
