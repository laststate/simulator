package scenario

import (
	"testing"
	"time"
)

func TestEngineRotatesPhases(t *testing.T) {
	phases := []Phase{
		{Kind: Steady, Duration: 10 * time.Second},
		{Kind: CrashStorm, Duration: 20 * time.Second},
		{Kind: NetworkOut, Duration: 5 * time.Second, Offline: true},
	}
	start := time.Now()
	e := NewEngine(phases)

	if e.Current().Kind != Steady {
		t.Fatalf("starts at %s, want steady", e.Current().Kind)
	}
	if e.Advance(start.Add(5 * time.Second)) {
		t.Fatal("advanced before deadline")
	}
	if !e.Advance(start.Add(11 * time.Second)) {
		t.Fatal("did not advance after deadline")
	}
	if e.Current().Kind != CrashStorm {
		t.Fatalf("phase = %s, want crash-storm", e.Current().Kind)
	}
	// Skip past crash-storm and network-out; wraps back to steady.
	if !e.Advance(start.Add(60 * time.Second)) {
		t.Fatal("no advance")
	}
	if e.Current().Kind != NetworkOut {
		t.Fatalf("phase = %s, want network-outage", e.Current().Kind)
	}
	if !e.Current().Offline {
		t.Fatal("network-outage phase not marked offline")
	}
	if !e.Advance(start.Add(80 * time.Second)) {
		t.Fatal("no advance")
	}
	if e.Current().Kind != Steady {
		t.Fatalf("script did not wrap: %s", e.Current().Kind)
	}
}

func TestSingleScenarioScripts(t *testing.T) {
	cases := map[Kind]func(Phase) bool{
		CrashStorm: func(p Phase) bool { return p.Behavior.ErrorRate == 0.9 },
		NetworkOut: func(p Phase) bool { return p.Offline },
		BootLoop:   func(p Phase) bool { return p.Behavior.BootLoop },
		Steady:     func(p Phase) bool { return !p.Offline && !p.Behavior.BootLoop },
	}
	for kind, check := range cases {
		script := Single(kind)
		if len(script) != 1 {
			t.Fatalf("%s: script len = %d", kind, len(script))
		}
		if script[0].Kind != kind || !check(script[0]) {
			t.Fatalf("%s: wrong script %+v", kind, script[0])
		}
	}
}

func TestDefaultScriptCoversAllPhases(t *testing.T) {
	script := DefaultScript()
	seen := map[Kind]bool{}
	for _, p := range script {
		seen[p.Kind] = true
		if p.Duration <= 0 || p.Note == "" {
			t.Fatalf("malformed phase %+v", p)
		}
	}
	for _, want := range []Kind{Steady, CrashStorm, NetworkOut, BootLoop} {
		if !seen[want] {
			t.Fatalf("default script missing %s", want)
		}
	}
}
