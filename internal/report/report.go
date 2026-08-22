// Package report renders the fleet state for humans (console tables) and
// machines (Prometheus metrics), so the demo shows exactly where everything
// is running and what each layer is doing.
package report

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/laststate/simulator/internal/device"
	"github.com/laststate/simulator/internal/lep"
)

// Metrics holds all simulator Prometheus series.
type Metrics struct {
	EventsGenerated *prometheus.CounterVec // {device,type}
	EventsSent      *prometheus.CounterVec // {device,transport}
	SendErrors      *prometheus.CounterVec // {device,transport}
	Acks            *prometheus.CounterVec // {status}
	BufferDepth     *prometheus.GaugeVec   // {device}
	Flushed         *prometheus.CounterVec // {device}
	Dropped         *prometheus.CounterVec // {device}
	OutageActive    prometheus.Gauge
	ScenarioInfo    *prometheus.GaugeVec // {phase}
}

// NewMetrics registers the series on the default registry.
func NewMetrics() *Metrics {
	m := &Metrics{
		EventsGenerated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "laststate_sim_events_generated_total",
			Help: "LEP envelopes generated, by device and event type",
		}, []string{"device", "type"}),
		EventsSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "laststate_sim_events_sent_total",
			Help: "Envelopes handed to a transport, by device and transport",
		}, []string{"device", "transport"}),
		SendErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "laststate_sim_send_errors_total",
			Help: "Transport failures (device goes offline on failure)",
		}, []string{"device", "transport"}),
		Acks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "laststate_sim_acks_total",
			Help: "LSAK acknowledgements received, by status",
		}, []string{"status"}),
		BufferDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "laststate_sim_buffer_depth",
			Help: "Envelopes parked offline per device",
		}, []string{"device"}),
		Flushed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "laststate_sim_flushed_total",
			Help: "Buffered envelopes delivered after reconnect",
		}, []string{"device"}),
		Dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "laststate_sim_dropped_total",
			Help: "Evidence dropped when the offline ring overflowed",
		}, []string{"device"}),
		OutageActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "laststate_sim_network_outage_active",
			Help: "1 while the scripted network outage phase is running",
		}),
		ScenarioInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "laststate_sim_scenario_phase",
			Help: "1 for the currently active scenario phase",
		}, []string{"phase"}),
	}
	prometheus.MustRegister(m.EventsGenerated, m.EventsSent, m.SendErrors,
		m.Acks, m.BufferDepth, m.Flushed, m.Dropped, m.OutageActive, m.ScenarioInfo)
	return m
}

// SetPhase updates scenario gauges.
func (m *Metrics) SetPhase(phase string) {
	m.ScenarioInfo.Reset()
	m.ScenarioInfo.WithLabelValues(phase).Set(1)
}

// ObserveAck records one LSAK.
func (m *Metrics) ObserveAck(status uint8) {
	m.Acks.WithLabelValues(lep.AckName(status)).Inc()
}

// ServeMetrics exposes /metrics on listenAddr (best effort, non-fatal).
func ServeMetrics(listenAddr string) {
	if listenAddr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(listenAddr, mux); err != nil {
			log.Printf("metrics server on %s: %v", listenAddr, err)
		}
	}()
}

// FleetTable prints the fleet state table everyone watches during demos.
func FleetTable(out *os.File, fleet []*device.Device, phase string, uptime string) {
	var b strings.Builder
	fmt.Fprintf(&b, "\n┌─ FLEET STATE ─ scenario: %s ─ uptime %s ─────────────────────────────┐\n", phase, uptime)
	fmt.Fprintf(&b, "│ %-22s %-5s %7s %7s %7s %7s %7s │\n", "DEVICE", "TRNSP", "SENT", "ACCEPT", "BUFFER", "FLUSH", "DROP")
	for _, d := range fleet {
		fmt.Fprintf(&b, "│ %-22s %-5s %7d %7d %7d %7d %7d │\n",
			truncate(d.ID(), 22), string(d.Transport),
			d.Stats.Sent.Load(), d.Stats.Accepted.Load(), d.BufferDepth(),
			d.Stats.Flushed.Load(), d.Stats.Dropped.Load())
	}
	var sent, acc, buf, fl, dr uint64
	for _, d := range fleet {
		sent += d.Stats.Sent.Load()
		acc += d.Stats.Accepted.Load()
		buf += uint64(d.BufferDepth())
		fl += d.Stats.Flushed.Load()
		dr += d.Stats.Dropped.Load()
	}
	fmt.Fprintf(&b, "│ %-22s %-5s %7d %7d %7d %7d %7d │\n", "TOTAL", "", sent, acc, buf, fl, dr)
	b.WriteString("└──────────────────────────────────────────────────────────────────────┘")
	fmt.Fprintln(out, b.String())
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
