// Package scenario drives the fleet through scripted behaviors so a single
// `docker compose up` demonstrates the whole platform: steady telemetry,
// crash storms landing in Trace, network outages exercising the offline-first
// guarantees, and boot-loop cascades.
package scenario

import (
	"fmt"
	"time"

	"github.com/laststate/simulator/internal/device"
)

// Kind identifies a scenario.
type Kind string

const (
	Steady     Kind = "steady"
	CrashStorm Kind = "crash-storm"
	NetworkOut Kind = "network-outage"
	BootLoop   Kind = "boot-loop"
)

// Phase is one scenario segment active for a fixed duration.
type Phase struct {
	Kind     Kind
	Duration time.Duration
	// Behavior applied to every device while this phase is active.
	Behavior device.Behavior
	// Offline marks the network as down for the whole fleet.
	Offline bool
	// Note explains what the observer should see in the platform UIs.
	Note string
}

// DefaultScript is the demo rotation: five phases that exercise every layer.
func DefaultScript() []Phase {
	return []Phase{
		{
			Kind:     Steady,
			Duration: 60 * time.Second,
			Behavior: device.Behavior{ErrorRate: 0.15},
			Note:     "steady fleet telemetry: heartbeats, logs and occasional faults flowing Relay -> Trace",
		},
		{
			Kind:     CrashStorm,
			Duration: 45 * time.Second,
			Behavior: device.Behavior{ErrorRate: 0.9},
			Note:     "crash storm: hard faults, asserts and watchdogs flood in; watch Trace group them into issues",
		},
		{
			Kind:     NetworkOut,
			Duration: 40 * time.Second,
			Behavior: device.Behavior{ErrorRate: 0.3},
			Offline:  true,
			Note:     "network outage: devices keep capturing evidence offline (buffered) while Relay delivery retries",
		},
		{
			Kind:     Steady,
			Duration: 30 * time.Second,
			Behavior: device.Behavior{ErrorRate: 0.3},
			Note:     "recovery: buffered events flush after reconnect; duplicates are deduplicated by event ID",
		},
		{
			Kind:     BootLoop,
			Duration: 30 * time.Second,
			Behavior: device.Behavior{ErrorRate: 0.2, BootLoop: true},
			Note:     "boot loop: reset cascades with rising boot counters, a classic firmware regression signature",
		},
	}
}

// Single builds a script that loops one scenario forever (for focused tests).
func Single(kind Kind) []Phase {
	switch kind {
	case CrashStorm:
		return []Phase{{Kind: CrashStorm, Duration: time.Hour, Behavior: device.Behavior{ErrorRate: 0.9}, Note: "continuous crash storm"}}
	case NetworkOut:
		return []Phase{{Kind: NetworkOut, Duration: time.Hour, Behavior: device.Behavior{ErrorRate: 0.3}, Offline: true, Note: "continuous outage"}}
	case BootLoop:
		return []Phase{{Kind: BootLoop, Duration: time.Hour, Behavior: device.Behavior{ErrorRate: 0.2, BootLoop: true}, Note: "continuous boot loop"}}
	default:
		return []Phase{{Kind: Steady, Duration: time.Hour, Behavior: device.Behavior{ErrorRate: 0.15}, Note: "steady telemetry"}}
	}
}

// Engine rotates through phases and reports transitions.
type Engine struct {
	Phases []Phase

	index    int
	deadline time.Time
}

// NewEngine creates an engine starting at phase 0.
func NewEngine(phases []Phase) *Engine {
	if len(phases) == 0 {
		phases = DefaultScript()
	}
	e := &Engine{Phases: phases}
	e.deadline = time.Now().Add(phases[0].Duration)
	return e
}

// Current returns the active phase.
func (e *Engine) Current() Phase { return e.Phases[e.index] }

// Advance moves to the next phase when the current one expires, wrapping
// around the script. It returns true when a transition happened.
func (e *Engine) Advance(now time.Time) bool {
	if now.Before(e.deadline) {
		return false
	}
	e.index = (e.index + 1) % len(e.Phases)
	e.deadline = now.Add(e.Phases[e.index].Duration)
	return true
}

// Describe renders the phase banner for logs.
func (p Phase) Describe() string {
	status := "online"
	if p.Offline {
		status = "OFFLINE"
	}
	return fmt.Sprintf("[%s] %s (fleet %s, error rate %.0f%%)", p.Kind, p.Note, status, p.Behavior.ErrorRate*100)
}
