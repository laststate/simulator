// Command simulator drives a fleet of virtual devices running Latch
// firmware against a real Relay gateway, demonstrating the full LastState
// platform: LEP encoding, every ingest transport, LSAK acknowledgements,
// offline buffering, and scripted incident scenarios.
//
// Layers exercised:
//
//	device fleet (this simulator)
//	    │ LEP v2 envelopes (CRC'd TLVs: identity, CPU/fault, breadcrumbs…)
//	    ├─ HTTP   → Relay HTTP source   (single + binary batch)
//	    ├─ TCP    → Relay TCP source    (COBS framing + LSAK ACK loop)
//	    └─ UDP    → Relay UDP source    (best-effort telemetry)
//	   Relay (offline-first gateway, persists before ACK, forwards…)
//	    └─→ Trace (backend: grouping, symbolication, dashboards)
package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/laststate/simulator/internal/device"
	"github.com/laststate/simulator/internal/lep"
	"github.com/laststate/simulator/internal/report"
	"github.com/laststate/simulator/internal/scenario"
	"github.com/laststate/simulator/internal/transport"
)

type config struct {
	RelayURL      string
	RelayToken    string
	TCPPort       string // relay TCP source host:port
	UDPPort       string // relay UDP source host:port
	RatePerSec    float64
	BatchSize     int
	DeviceCount   int
	ErrorRate     float64
	Scenario      string
	MetricsListen string
	ReportEvery   time.Duration
	BufferCap     int
}

func loadConfig() config {
	cfg := config{
		RelayURL:      envOr("RELAY_URL", "http://relay:8384"),
		RelayToken:    envOr("RELAY_TOKEN", "dev-ingest-token-12345"),
		TCPPort:       envOr("SIM_TCP_ADDR", "relay:8385"),
		UDPPort:       envOr("SIM_UDP_ADDR", "relay:8386"),
		RatePerSec:    envFloat("SIMULATOR_RATE", 4.0),
		BatchSize:     int(envFloat("SIMULATOR_BATCH_SIZE", 5)),
		DeviceCount:   int(envFloat("SIMULATOR_DEVICE_COUNT", 5)),
		ErrorRate:     envFloat("SIMULATOR_ERROR_RATE", 0.35),
		Scenario:      envOr("SIM_SCENARIO", "demo"),
		MetricsListen: envOr("SIM_METRICS_LISTEN", ":9468"),
		ReportEvery:   15 * time.Second,
		BufferCap:     int(envFloat("SIM_BUFFER_CAP", 256)),
	}
	if cfg.RatePerSec <= 0 {
		cfg.RatePerSec = 4
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 5
	}
	if cfg.DeviceCount <= 0 {
		cfg.DeviceCount = 5
	}
	if cfg.ErrorRate < 0 || cfg.ErrorRate > 1 {
		cfg.ErrorRate = 0.35
	}
	return cfg
}

func main() {
	cfg := loadConfig()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	log.SetFlags(log.Ltime)
	log.Printf("╔══════════════════════════════════════════════════════════════╗")
	log.Printf("║        LastState Platform Fleet Simulator (demo mode)        ║")
	log.Printf("╚══════════════════════════════════════════════════════════════╝")
	log.Printf("Relay HTTP : %s", cfg.RelayURL)
	log.Printf("Relay TCP  : %s (COBS + LSAK)", cfg.TCPPort)
	log.Printf("Relay UDP  : %s (best effort)", cfg.UDPPort)
	log.Printf("Devices    : %d   Rate: %.1f ev/s   Scenario: %s", cfg.DeviceCount, cfg.RatePerSec, cfg.Scenario)
	log.Printf("Metrics    : http://localhost%s/metrics", cfg.MetricsListen)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	metrics := report.NewMetrics()
	report.ServeMetrics(cfg.MetricsListen)

	// Gate on the Relay HTTP source so `docker compose up` ordering is safe.
	httpTransport := transport.NewHTTP(cfg.RelayURL, cfg.RelayToken)
	waitForRelay(ctx, httpTransport)

	// ACK-capable TCP transport shared by TCP devices; LSAK frames credit
	// the originating device asynchronously.
	tracker := newAckTracker()
	tcpTransport := transport.NewTCP(cfg.TCPPort, func(eventID uint32, status uint8) {
		metrics.ObserveAck(status)
		tracker.settled(eventID, status)
	})
	defer tcpTransport.Close()

	senders := map[device.TransportKind]transport.Sender{
		device.TransportHTTP: httpTransport,
		device.TransportTCP:  tcpTransport,
	}
	if udpTransport, err := transport.NewUDP(cfg.UDPPort); err != nil {
		log.Printf("UDP transport disabled: %v", err)
	} else {
		senders[device.TransportUDP] = udpTransport
	}

	var script []scenario.Phase
	if s := scenario.Kind(cfg.Scenario); s == "demo" {
		script = scenario.DefaultScript()
	} else {
		script = scenario.Single(s)
	}
	engine := scenario.NewEngine(script)
	metrics.SetPhase(string(engine.Current().Kind))
	log.Printf("▶ phase %s", engine.Current().Describe())

	fleet := device.DefaultFleet(cfg.DeviceCount, cfg.BufferCap)
	for _, d := range fleet {
		d.Online.Store(true)
		tracker.attach(d)
	}
	start := time.Now()

	// Event loop: one ticker drives the whole fleet.
	ticker := time.NewTicker(time.Duration(float64(time.Second) / cfg.RatePerSec))
	defer ticker.Stop()

	reportTicker := time.NewTicker(cfg.ReportEvery)
	defer reportTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			report.FleetTable(os.Stdout, fleet, string(engine.Current().Kind), time.Since(start).Round(time.Second).String())
			log.Printf("simulator stopped after %s", time.Since(start).Round(time.Second))
			return

		case <-reportTicker.C:
			report.FleetTable(os.Stdout, fleet, string(engine.Current().Kind), time.Since(start).Round(time.Second).String())

		case <-ticker.C:
			if engine.Advance(time.Now()) {
				phase := engine.Current()
				metrics.SetPhase(string(phase.Kind))
				metrics.OutageActive.Set(boolGauge(phase.Offline))
				log.Printf("▶ phase %s", phase.Describe())
				for _, d := range fleet {
					d.Online.Store(!phase.Offline)
				}
				continue
			}

			phase := engine.Current()
			dev := fleet[rng.Intn(len(fleet))]

			// Reconnect path: flush offline buffers first.
			if dev.Online.Load() && dev.BufferDepth() > 0 {
				flush(ctx, dev, senders, metrics)
				continue
			}

			isFault := rng.Float64() < phase.Behavior.ErrorRate
			forceBoot := phase.Behavior.BootLoop && rng.Float64() < 0.8
			kind := device.FaultNone
			if isFault && !forceBoot {
				switch rng.Intn(5) {
				case 0, 1:
					kind = device.FaultHardFault
				case 2:
					kind = device.FaultAssert
				case 3:
					kind = device.FaultWatchdog
				default:
					kind = device.FaultPeripheral
				}
			}
			env := dev.Generate(kind, forceBoot)
			metrics.EventsGenerated.WithLabelValues(dev.ID(), lepTypeName(env.Type)).Inc()

			if !dev.Online.Load() {
				dev.Park(env)
				metrics.BufferDepth.WithLabelValues(dev.ID()).Set(float64(dev.BufferDepth()))
				log.Printf("  ⧗ [%s] offline: evidence #%d buffered (%d parked)", dev.ID(), env.Seq, dev.BufferDepth())
				continue
			}

			// Occasional batch over HTTP (mirror real fleet burst behavior).
			if dev.Transport == device.TransportHTTP && rng.Float64() < 0.15 && cfg.BatchSize > 1 {
				sendBatch(ctx, fleet, httpTransport, metrics, cfg.BatchSize, phase.Behavior.ErrorRate)
				continue
			}

			deliver(ctx, dev, env, senders[dev.Transport], metrics, tracker)
		}
	}
}

// ackTracker correlates LSAK acknowledgements with the device that sent the
// envelope, crediting the Accepted counter on the TCP (ACK-capable) path.
type ackTracker struct {
	mu   sync.Mutex
	devs map[string]*device.Device
	pend map[uint32]*device.Device
}

func newAckTracker() *ackTracker {
	return &ackTracker{devs: make(map[string]*device.Device), pend: make(map[uint32]*device.Device)}
}

func (t *ackTracker) attach(d *device.Device) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.devs[d.ID()] = d
}

func (t *ackTracker) awaiting(env *lep.Envelope, d *device.Device) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pend[env.EventID] = d
}

func (t *ackTracker) settled(eventID uint32, status uint8) {
	t.mu.Lock()
	dev, known := t.pend[eventID]
	if known {
		delete(t.pend, eventID)
	}
	t.mu.Unlock()
	if !known {
		return
	}
	if status == lep.AckStored || status == lep.AckDuplicate {
		dev.Stats.Accepted.Add(1)
	}
	log.Printf("  ✓ [lsak] %s event %08x: %s", dev.ID(), eventID, lep.AckName(status))
}

func deliver(ctx context.Context, dev *device.Device, env *lep.Envelope, sender transport.Sender, m *report.Metrics, tracker *ackTracker) {
	if sender == nil {
		dev.Park(env)
		return
	}
	dev.Stats.Sent.Add(1)
	m.EventsSent.WithLabelValues(dev.ID(), string(dev.Transport)).Inc()

	if dev.Transport == device.TransportTCP {
		tracker.awaiting(env, dev)
	}

	err := sender.Send(ctx, env)
	switch {
	case err == nil:
		if dev.Transport != device.TransportTCP {
			// HTTP/UDP have no async ACK loop; delivery succeeded.
			dev.Stats.Accepted.Add(1)
		}
		log.Printf("  ⚡ [%s|%s] %s", dev.ID(), dev.Transport, env.Summary)
	case ctx.Err() != nil:
		return
	default:
		m.SendErrors.WithLabelValues(dev.ID(), string(dev.Transport)).Inc()
		dev.Park(env)
		m.BufferDepth.WithLabelValues(dev.ID()).Set(float64(dev.BufferDepth()))
		log.Printf("  ✗ [%s|%s] send failed (%v); buffered %d events", dev.ID(), dev.Transport, err, dev.BufferDepth())
	}
}

func flush(ctx context.Context, dev *device.Device, senders map[device.TransportKind]transport.Sender, m *report.Metrics) {
	parked := dev.Drain()
	sender := senders[dev.Transport]
	ok := 0
	for _, env := range parked {
		if sender == nil {
			dev.Park(env)
			continue
		}
		dev.Stats.Sent.Add(1)
		if err := sender.Send(ctx, env); err != nil {
			if ctx.Err() != nil {
				return
			}
			dev.Park(env)
			continue
		}
		ok++
	}
	if ok > 0 {
		dev.Stats.Flushed.Add(uint64(ok))
		m.Flushed.WithLabelValues(dev.ID()).Add(float64(ok))
		log.Printf("  ↻ [%s] flushed %d buffered events after reconnect", dev.ID(), ok)
	}
	m.BufferDepth.WithLabelValues(dev.ID()).Set(float64(dev.BufferDepth()))
}

func sendBatch(ctx context.Context, fleet []*device.Device, batcher transport.BatchSender, m *report.Metrics, size int, errorRate float64) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	envelopes := make([]*lep.Envelope, 0, size)
	devs := make([]*device.Device, 0, size)
	for i := 0; i < size; i++ {
		dev := fleet[rng.Intn(len(fleet))]
		kind := device.FaultNone
		if rng.Float64() < errorRate {
			kind = device.FaultHardFault
		}
		env := dev.Generate(kind, false)
		envelopes = append(envelopes, env)
		devs = append(devs, dev)
	}
	if err := batcher.SendBatch(ctx, envelopes); err != nil {
		if ctx.Err() != nil {
			return
		}
		for i, env := range envelopes {
			devs[i].Park(env)
		}
		m.SendErrors.WithLabelValues("fleet", "http-batch").Inc()
		log.Printf("  ✗ [batch] failed (%v); events buffered", err)
		return
	}
	for _, dev := range devs {
		dev.Stats.Sent.Add(1)
		dev.Stats.Accepted.Add(1)
	}
	log.Printf("  ⚡ [BATCH x%d] fleet burst delivered", size)
}

func waitForRelay(ctx context.Context, httpTransport *transport.HTTP) {
	log.Printf("waiting for Relay gateway…")
	for {
		if ctx.Err() != nil {
			return
		}
		if err := httpTransport.Capabilities(ctx); err == nil {
			log.Printf("✓ Relay is up (capabilities ok) — fleet going online")
			return
		}
		time.Sleep(time.Second)
	}
}

func lepTypeName(t uint8) string {
	switch t {
	case lep.TypeCrash:
		return "crash"
	case lep.TypeError:
		return "error"
	case lep.TypeHealth:
		return "health"
	case lep.TypeReset:
		return "reset"
	case lep.TypeLog:
		return "log"
	case lep.TypePeripheral:
		return "peripheral"
	}
	return "other"
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}
