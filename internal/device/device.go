// Package device models simulated embedded devices running Latch firmware:
// identity, telemetry drift (battery, heap, temperature), realistic fault
// generation, and an offline ring buffer that mirrors Latch's
// persist-before-ACK evidence capture.
package device

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/laststate/simulator/internal/lep"
)

// Profile is the static identity of one device model.
type Profile struct {
	DeviceID     string
	Product      string
	HwRev        string
	FwVersion    string
	BuildID      string
	GitCommit    string
	Architecture uint8
	ArchName     string
	RTOS         string
	Region       string
}

// TransportKind selects how a device reaches the gateway.
type TransportKind string

const (
	TransportHTTP TransportKind = "http"
	TransportTCP  TransportKind = "tcp"
	TransportUDP  TransportKind = "udp"
)

// Stats counters for reporting; updated atomically.
type Stats struct {
	Sent     atomic.Uint64
	Accepted atomic.Uint64
	Buffered atomic.Uint64
	Flushed  atomic.Uint64
	Dropped  atomic.Uint64
}

// Device is one simulated unit with mutable runtime state.
type Device struct {
	Profile   Profile
	Transport TransportKind

	Seq       uint32
	BootCount uint32
	BatteryMV int32 // millivolts, drifts down and recharges after boot
	HeapFree  uint32
	TempC     float64

	Online  atomic.Bool // false during network-outage scenarios
	Stats   Stats
	Created time.Time

	mu     sync.Mutex
	buffer []*lep.Envelope
	bufCap int
	rng    *rand.Rand
}

// New creates a device with a deterministic-per-device RNG.
func New(profile Profile, transport TransportKind, bufferCap int) *Device {
	if bufferCap <= 0 {
		bufferCap = 256
	}
	return &Device{
		Profile:   profile,
		Transport: transport,
		BatteryMV: 3300 + int32(rand.Intn(300)),
		HeapFree:  40000 + uint32(rand.Intn(8000)),
		TempC:     22 + rand.Float64()*6,
		BootCount: uint32(rand.Intn(40)),
		Created:   time.Now(),
		bufCap:    bufferCap,
		rng:       rand.New(rand.NewSource(int64(crc32.ChecksumIEEE([]byte(profile.DeviceID))))),
	}
}

// ID returns the device identifier.
func (d *Device) ID() string { return d.Profile.DeviceID }

// BufferDepth returns how many envelopes are parked offline.
func (d *Device) BufferDepth() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.buffer)
}

// Park stores an envelope offline (network down or send failure).
// When the ring is full the oldest evidence is dropped, exactly like a
// constrained flash slot: newer evidence wins.
func (d *Device) Park(env *lep.Envelope) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.buffer = append(d.buffer, env)
	if len(d.buffer) > d.bufCap {
		d.buffer = d.buffer[1:]
		d.Stats.Dropped.Add(1)
	}
	d.Stats.Buffered.Add(1)
}

// Drain returns parked envelopes in FIFO order and clears the buffer.
func (d *Device) Drain() []*lep.Envelope {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := d.buffer
	d.buffer = nil
	return out
}

// FaultKind enumerates the failure catalog the fleet can produce.
type FaultKind int

const (
	FaultNone FaultKind = iota
	FaultHardFault
	FaultAssert
	FaultWatchdog
	FaultPeripheral
	FaultBrownout
)

// Behavior steers event generation for the current scenario.
type Behavior struct {
	ErrorRate float64 // 0..1 probability of a fault event
	BootLoop  bool    // devices constantly reboot
}

// Generate produces the next event for the device. isFault decides the
// category; callers usually derive it from Behavior.ErrorRate. The whole
// body runs under the device lock so concurrent schedulers are safe.
func (d *Device) Generate(kind FaultKind, forceBoot bool) *lep.Envelope {
	d.mu.Lock()
	defer d.mu.Unlock()

	rng := d.rng
	d.Seq++
	seq := d.Seq

	eventID := rng.Uint32()
	tlvs := []lep.TLV{{Type: lep.TLVIdentity, Value: d.encodeIdentityLocked()}}

	var eventType uint8
	var summary string

	if forceBoot {
		d.BootCount++
		d.BatteryMV = 3300 + int32(rng.Intn(150)) // fresh boot: recharged rail
		eventType = lep.TypeReset
		summary = fmt.Sprintf("BOOT: power-on reset #%d (%s)", d.BootCount, d.ID())
		tlvs = append(tlvs, tlvReset(1, d.BootCount), tlvBreadcrumb(rng, 1, "boot", "system init complete"))
	} else {
		switch kind {
		case FaultHardFault:
			eventType = lep.TypeCrash
			summary = "CRASH: HardFault (CFSR DACCVIOL / DIVBYZERO)"
			tlvs = append(tlvs, d.crashTLVsLocked(rng)...)
		case FaultAssert:
			eventType = lep.TypeError
			msg := []string{
				"assert(buf_len <= MAX_PAYLOAD_BYTES) failed at src/net/transport.c:184",
				"assert(p_queue != NULL) failed at drivers/i2c/i2c_master.c:92",
				"assert(dma_busy == 0) timeout at hal/stm32/spi_dma.c:304",
				"assert(sensor_status == SENSOR_OK) at sensors/bme280.c:75",
			}[rng.Intn(4)]
			summary = "ASSERT: " + msg
			tlvs = append(tlvs, lep.TLV{Type: lep.TLVAssert, Value: []byte(msg)})
			tlvs = append(tlvs, breadcrumbs(rng, 3)...)
		case FaultWatchdog:
			eventType = lep.TypeReset
			d.BootCount++
			summary = "RESET: watchdog timeout (IWDG) across reboot"
			tlvs = append(tlvs, tlvReset(4, d.BootCount), tlvBreadcrumb(rng, 2, "wdt", "main loop stalled"))
			tlvs = append(tlvs, breadcrumbs(rng, 3)...)
		case FaultBrownout:
			eventType = lep.TypeReset
			d.BootCount++
			summary = "RESET: brownout (supply sag below BOR threshold)"
			tlvs = append(tlvs, tlvReset(3, d.BootCount), tlvBreadcrumb(rng, 2, "pwr", "rail sagged"))
		case FaultPeripheral:
			eventType = lep.TypePeripheral
			summary = "PERIPHERAL: I2C ACK timeout on addr 0x68 (IMU)"
			tlvs = append(tlvs,
				lep.TLV{Type: lep.TLVLog, Value: []byte(fmt.Sprintf("E i2c: arbitration lost on bus 1 (tick %d)", time.Now().UnixMilli()%1_000_000))},
				tlvHeap(12400, 4096, 120, 110))
			tlvs = append(tlvs, breadcrumbs(rng, 2)...)
		default:
			d.tickTelemetryLocked(rng)
			switch rng.Intn(3) {
			case 0:
				eventType = lep.TypeHealth
				summary = fmt.Sprintf("HEALTH: batt %dmV heap %dKB free temp %.1fC", d.BatteryMV, d.HeapFree/1024, d.TempC)
				tlvs = append(tlvs,
					tlvHeap(d.HeapFree, d.HeapFree/10, 450, 430),
					lep.TLV{Type: lep.TLVMetric, Value: tlvMetric("battery_mv", int(d.BatteryMV))},
					lep.TLV{Type: lep.TLVMetric, Value: tlvMetric("temp_c", int(d.TempC*10))})
			case 1:
				eventType = lep.TypeLog
				msg := []string{
					"I (wifi) connected to SSID 'Factory_IoT_Mesh' (RSSI -54 dBm)",
					"I (ota) firmware check complete: version up-to-date",
					"I (sensor) ambient 23.4C humidity 58%% pressure 1013 hPa",
					"I (mqtt) keepalive ACK (latency 18ms)",
				}[rng.Intn(4)]
				summary = "LOG: " + msg
				tlvs = append(tlvs, lep.TLV{Type: lep.TLVLog, Value: []byte(msg)})
			default:
				eventType = lep.TypeReset
				d.BootCount++
				summary = fmt.Sprintf("BOOT: scheduled maintenance reboot #%d", d.BootCount)
				tlvs = append(tlvs, tlvReset(2, d.BootCount))
			}
		}
	}

	raw := lep.Encode(lep.Version2, eventType, d.Profile.Architecture, 0, seq, eventID, tlvs)
	return &lep.Envelope{Raw: raw, EventID: eventID, Seq: seq, Type: eventType, Summary: summary}
}

// tickTelemetryLocked drifts health values; caller must hold d.mu.
func (d *Device) tickTelemetryLocked(rng *rand.Rand) {
	d.BatteryMV -= int32(rng.Intn(6))
	if d.BatteryMV < 2900 {
		d.BatteryMV = 3300 // battery swap, not modeled
	}
	d.HeapFree = uint32(int(d.HeapFree) + rng.Intn(1024) - 512)
	if d.HeapFree < 8192 {
		d.HeapFree = 8192
	}
	d.TempC += rng.Float64()*0.8 - 0.4
}

// crashTLVsLocked builds CPU/fault context; caller must hold d.mu.
func (d *Device) crashTLVsLocked(rng *rand.Rand) []lep.TLV {
	cpu := make([]byte, 12)
	cpu[0] = d.Profile.Architecture
	cpu[1] = 1                                                                    // HardFault
	binary.LittleEndian.PutUint32(cpu[4:8], uint32(0x08001000+rng.Intn(0x10000))) // PC
	binary.LittleEndian.PutUint32(cpu[8:12], uint32(0x08000800+rng.Intn(0x4000))) // LR

	fault := make([]byte, 24)
	cfsr := uint32(0x00008200) // DACCVIOL + MMARVALID
	if rng.Intn(2) == 0 {
		cfsr = 1 << 25 // DIVBYZERO
	}
	binary.LittleEndian.PutUint32(fault[0:4], cfsr)
	binary.LittleEndian.PutUint32(fault[4:8], 0x40000000)  // HFSR.FORCED
	binary.LittleEndian.PutUint32(fault[8:12], 0x20004500) // MMAR

	return append([]lep.TLV{
		{Type: lep.TLVCPU, Value: cpu},
		{Type: lep.TLVFault, Value: fault},
	}, breadcrumbs(rng, 4)...)
}

// encodeIdentityLocked renders the Identity TLV nested-string format;
// caller must hold d.mu.
func (d *Device) encodeIdentityLocked() []byte {
	var buf strings.Builder
	fields := []struct {
		id  uint8
		val string
	}{
		{1, "default"},
		{2, d.Profile.DeviceID},
		{3, d.Profile.Product},
		{4, d.Profile.HwRev},
		{7, d.Profile.FwVersion},
		{8, d.Profile.BuildID},
		{10, d.Profile.GitCommit},
		{12, d.Profile.ArchName},
		{13, d.Profile.RTOS},
		{14, d.Profile.Region},
	}
	for _, f := range fields {
		if f.val == "" || len(f.val) > 255 {
			continue
		}
		buf.WriteByte(f.id)
		buf.WriteByte(byte(len(f.val)))
		buf.WriteString(f.val)
	}
	return []byte(buf.String())
}

func breadcrumbs(rng *rand.Rand, count int) []lep.TLV {
	categories := []string{"sensor", "flash", "net", "motor", "calc"}
	actions := []string{"read_i2c", "write_sector", "mqtt_publish", "set_pwm", "compute_fft", "allocate_dma"}
	out := make([]lep.TLV, 0, count)
	for i := 0; i < count; i++ {
		cat := categories[rng.Intn(len(categories))]
		msg := cat + ":" + actions[rng.Intn(len(actions))]
		out = append(out, tlvBreadcrumb(rng, uint8(i+1), cat, msg))
	}
	return out
}

func tlvBreadcrumb(rng *rand.Rand, sev uint8, cat, msg string) lep.TLV {
	value := make([]byte, 0, 15+2+len(cat)+2+len(msg))
	var hdr [15]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(time.Now().UnixMilli()%1_000_000))
	binary.LittleEndian.PutUint16(hdr[4:6], uint16(rng.Intn(1000)))
	hdr[6] = sev
	binary.LittleEndian.PutUint32(hdr[7:11], crc32.ChecksumIEEE([]byte(cat)))
	binary.LittleEndian.PutUint32(hdr[11:15], crc32.ChecksumIEEE([]byte(msg)))
	value = append(value, hdr[:]...)
	value = append(value, 1, byte(len(cat)))
	value = append(value, cat...)
	value = append(value, 2, byte(len(msg)))
	value = append(value, msg...)
	return lep.TLV{Type: lep.TLVBreadcrumb, Value: value}
}

func tlvReset(reason uint8, bootCount uint32) lep.TLV {
	b := make([]byte, 20)
	b[0] = reason
	binary.LittleEndian.PutUint32(b[1:5], uint32(reason))
	binary.LittleEndian.PutUint32(b[5:9], bootCount)
	binary.LittleEndian.PutUint32(b[9:13], 125000) // previous uptime ms
	binary.LittleEndian.PutUint32(b[13:17], uint32(time.Now().UnixMilli()%1_000_000))
	if reason == 4 {
		b[18] = 1 // previous_crashed
	}
	return lep.TLV{Type: lep.TLVReset, Value: b}
}

func tlvHeap(free, minFree, allocs, frees uint32) lep.TLV {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint32(b[0:4], free)
	binary.LittleEndian.PutUint32(b[4:8], minFree)
	binary.LittleEndian.PutUint32(b[8:12], allocs)
	binary.LittleEndian.PutUint32(b[12:16], frees)
	return lep.TLV{Type: lep.TLVHeap, Value: b}
}

func tlvMetric(name string, val int) []byte {
	return []byte(name + ":" + strconv.Itoa(val))
}

// DefaultFleet builds the demo roster: five archetypes spanning the
// architectures the platform supports, each pinned to a transport so the
// demo exercises every gateway source.
func DefaultFleet(count int, bufferCap int) []*Device {
	templates := []struct {
		profile   Profile
		transport TransportKind
	}{
		{
			profile:   Profile{"stm32-meter-001", "smart-meter-pro", "rev-c2", "v2.3.1", "stm32f4-build-9801", "e7a8b9c", lep.ArchCortexM, "ARM Cortex-M4", "FreeRTOS v10.4", "sa-east-1"},
			transport: TransportTCP,
		},
		{
			profile:   Profile{"esp32-gateway-002", "iot-gateway-hub", "v3.0", "v1.4.0", "esp32-idf-4412", "a1b2c3d", lep.ArchXtensa, "Xtensa LX6", "ESP-IDF FreeRTOS", "us-east-1"},
			transport: TransportHTTP,
		},
		{
			profile:   Profile{"nrf52-beacon-003", "asset-tracker-tag", "rev-b", "v3.1.2", "nrf52840-zephyr-5510", "f4e3d2c", lep.ArchCortexM, "ARM Cortex-M4F", "Zephyr RTOS 3.5", "eu-west-1"},
			transport: TransportUDP,
		},
		{
			profile:   Profile{"riscv-solar-004", "solar-inverter-ctrl", "v1.1", "v0.9.8", "rv32imac-fw-102", "1122334", lep.ArchRISCV, "RISC-V RV32IMAC", "FreeRTOS RISC-V", "ap-southeast-1"},
			transport: TransportTCP,
		},
		{
			profile:   Profile{"linux-edge-node-005", "edge-ai-aggregator", "rpi-cm4-v2", "v4.0.5", "linux-yocto-6.6", "deadbeef99", lep.ArchX86_64, "Linux x86/ARM64", "Embedded Linux", "us-west-2"},
			transport: TransportHTTP,
		},
	}

	devices := make([]*Device, 0, count)
	for i := 0; i < count; i++ {
		t := templates[i%len(templates)]
		profile := t.profile
		if i >= len(templates) {
			profile.DeviceID = fmt.Sprintf("%s-%03d", t.profile.Product, i+1)
		}
		devices = append(devices, New(profile, t.transport, bufferCap))
	}
	return devices
}
