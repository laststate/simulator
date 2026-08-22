// Package lep encodes LEP (Last State Protocol) v1/v2 envelopes exactly as
// the firmware-side Latch library would, per the wire format specified in
// the laststate/protocol repository.
//
// Envelope layout (little-endian):
//
//	 0..4   magic "LSTP"
//	 4      version (1 or 2)
//	 5      event type
//	 6      architecture code
//	 7      flags (bit0 authenticated, bit1 encrypted, bit2 AEAD, bit3 compressed)
//	 8..12  sequence (per device, monotonic)
//	12..16  event ID (random, echoed by LSAK ACKs)
//	16..20  payload length
//	20..24  header CRC-32/IEEE over bytes 0..20
//	24..    TLV payload
//	last 4  payload CRC-32/IEEE
package lep

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	HeaderSize = 24
	TrailerCRC = 4
	Magic      = "LSTP"
	Version1   = 1
	Version2   = 2
)

// Event types.
const (
	TypeCrash      uint8 = 1
	TypeError      uint8 = 2
	TypeMessage    uint8 = 3
	TypeHealth     uint8 = 4
	TypeReset      uint8 = 5
	TypeLog        uint8 = 6
	TypePeripheral uint8 = 7
	TypeCoredump   uint8 = 8
)

// Architecture codes (protocol registry).
const (
	ArchCortexM uint8 = 1
	ArchCortexA uint8 = 2
	ArchRISCV   uint8 = 3
	ArchXtensa  uint8 = 4
	ArchX86_64  uint8 = 5
)

// TLV types (protocol registry).
const (
	TLVIdentity   uint16 = 1
	TLVReset      uint16 = 2
	TLVEvent      uint16 = 3
	TLVCPU        uint16 = 4
	TLVFault      uint16 = 5
	TLVBreadcrumb uint16 = 6
	TLVMetric     uint16 = 7
	TLVPower      uint16 = 8
	TLVHealth     uint16 = 9
	TLVAssert     uint16 = 10
	TLVPeripheral uint16 = 11
	TLVLog        uint16 = 12
	TLVMemory     uint16 = 13
	TLVStack      uint16 = 14
	TLVHeap       uint16 = 15
)

// Flags.
const (
	FlagCompressed uint8 = 1 << 3
)

// TLV is one payload record.
type TLV struct {
	Type  uint16
	Value []byte
}

// Envelope is an encoded LEP event plus the identity the ACK path needs.
type Envelope struct {
	Raw     []byte
	EventID uint32
	Seq     uint32
	Type    uint8
	Summary string
}

// ErrBadEnvelope is returned by Validate on structural corruption.
var ErrBadEnvelope = errors.New("lep: malformed envelope")

// EncodeTLVs packs TLV records: type u16 | length u16 | value.
func EncodeTLVs(tlvs []TLV) []byte {
	size := 0
	for _, t := range tlvs {
		size += 4 + len(t.Value)
	}
	out := make([]byte, 0, size)
	var hdr [4]byte
	for _, t := range tlvs {
		binary.LittleEndian.PutUint16(hdr[0:2], t.Type)
		binary.LittleEndian.PutUint16(hdr[2:4], uint16(len(t.Value)))
		out = append(out, hdr[:]...)
		out = append(out, t.Value...)
	}
	return out
}

// Encode builds a complete LEP envelope. The sequence and event ID are
// supplied by the caller (device state owns monotonicity).
func Encode(version uint8, eventType, arch, flags uint8, seq, eventID uint32, tlvs []TLV) []byte {
	payload := EncodeTLVs(tlvs)

	raw := make([]byte, HeaderSize+len(payload)+TrailerCRC)
	copy(raw[0:4], Magic)
	raw[4] = version
	raw[5] = eventType
	raw[6] = arch
	raw[7] = flags
	binary.LittleEndian.PutUint32(raw[8:12], seq)
	binary.LittleEndian.PutUint32(raw[12:16], eventID)
	binary.LittleEndian.PutUint32(raw[16:20], uint32(len(payload)))
	binary.LittleEndian.PutUint32(raw[20:24], crc32.ChecksumIEEE(raw[:20]))

	copy(raw[HeaderSize:], payload)
	binary.LittleEndian.PutUint32(raw[HeaderSize+len(payload):], crc32.ChecksumIEEE(payload))
	return raw
}

// Validate checks magic, version, declared length and both CRCs. It returns
// ErrBadEnvelope describing the first structural problem found.
func Validate(raw []byte) error {
	if len(raw) < HeaderSize+TrailerCRC {
		return ErrBadEnvelope
	}
	if string(raw[0:4]) != Magic {
		return ErrBadEnvelope
	}
	if v := raw[4]; v != Version1 && v != Version2 {
		return ErrBadEnvelope
	}
	payloadLen := int(binary.LittleEndian.Uint32(raw[16:20]))
	if payloadLen < 0 || HeaderSize+payloadLen+TrailerCRC != len(raw) {
		return ErrBadEnvelope
	}
	if crc32.ChecksumIEEE(raw[:20]) != binary.LittleEndian.Uint32(raw[20:24]) {
		return ErrBadEnvelope
	}
	payload := raw[HeaderSize : HeaderSize+payloadLen]
	if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(raw[HeaderSize+payloadLen:]) {
		return ErrBadEnvelope
	}
	return nil
}

// DecodeEventID extracts the event ID from a raw envelope (echoed in ACKs).
func DecodeEventID(raw []byte) (uint32, error) {
	if len(raw) < HeaderSize {
		return 0, ErrBadEnvelope
	}
	return binary.LittleEndian.Uint32(raw[12:16]), nil
}

// LSAK (Last State ACK) framing used by acknowledgement-capable transports.
const (
	LSAKMagic    = "LSAK"
	LSAKSize     = 12
	LSAKVersion  = 1
	AckStored    = 1
	AckDuplicate = 2
	NackCorrupt  = 3
	NackUnsup    = 4
	NackBusy     = 5
	NackTooLarge = 6
	NackUnauth   = 7
	NackInternal = 8
)

// ParseLSAK decodes one LSAK frame into (eventID, status).
func ParseLSAK(frame []byte) (uint32, uint8, error) {
	if len(frame) != LSAKSize || string(frame[0:4]) != LSAKMagic || frame[4] != LSAKVersion {
		return 0, 0, ErrBadEnvelope
	}
	return binary.LittleEndian.Uint32(frame[8:12]), frame[5], nil
}

// AckName maps an LSAK status to a human label for logs and dashboards.
func AckName(status uint8) string {
	switch status {
	case AckStored:
		return "stored"
	case AckDuplicate:
		return "duplicate"
	case NackCorrupt:
		return "nack-corrupt"
	case NackUnsup:
		return "nack-unsupported"
	case NackBusy:
		return "nack-busy"
	case NackTooLarge:
		return "nack-too-large"
	case NackUnauth:
		return "nack-unauthorized"
	case NackInternal:
		return "nack-internal"
	}
	return "unknown"
}
