package lep

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestEncodeValidateRoundTrip(t *testing.T) {
	tlvs := []TLV{
		{Type: TLVIdentity, Value: []byte("identity-blob")},
		{Type: TLVHeap, Value: []byte{0, 0, 0, 0}},
	}
	raw := Encode(Version2, TypeCrash, ArchCortexM, 0, 42, 0xDEADBEEF, tlvs)

	if err := Validate(raw); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	id, err := DecodeEventID(raw)
	if err != nil || id != 0xDEADBEEF {
		t.Fatalf("DecodeEventID = %x, %v", id, err)
	}
	if seq := binary.LittleEndian.Uint32(raw[8:12]); seq != 42 {
		t.Fatalf("seq = %d, want 42", seq)
	}
	if raw[5] != TypeCrash || raw[6] != ArchCortexM {
		t.Fatalf("type/arch = %d/%d", raw[5], raw[6])
	}
}

func TestValidateRejectsCorruption(t *testing.T) {
	raw := Encode(Version1, TypeHealth, ArchRISCV, 0, 1, 2, []TLV{{Type: TLVLog, Value: []byte("hi")}})

	corrupt := append([]byte(nil), raw...)
	corrupt[24] ^= 0xFF // payload bit flip
	if err := Validate(corrupt); err == nil {
		t.Fatal("payload corruption not detected")
	}

	corrupt = append([]byte(nil), raw...)
	corrupt[5] ^= 0xFF // header field change breaks header CRC
	if err := Validate(corrupt); err == nil {
		t.Fatal("header corruption not detected")
	}

	corrupt = append([]byte(nil), raw...)
	binary.LittleEndian.PutUint32(corrupt[16:20], 999) // length mismatch
	if err := Validate(corrupt); err == nil {
		t.Fatal("length mismatch not detected")
	}
}

func TestValidateRejectsBadMagicAndVersion(t *testing.T) {
	raw := Encode(Version2, TypeLog, ArchXtensa, 0, 1, 2, nil)
	raw[0] = 'X'
	if err := Validate(raw); err == nil {
		t.Fatal("bad magic not detected")
	}
	raw = Encode(9, TypeLog, ArchXtensa, 0, 1, 2, nil)
	if err := Validate(raw); err == nil {
		t.Fatal("unknown version not detected")
	}
}

func TestParseLSAK(t *testing.T) {
	frame := []byte{'L', 'S', 'A', 'K', 1, AckStored, 0, 0, 0xEF, 0xBE, 0xAD, 0xDE}
	id, status, err := ParseLSAK(frame)
	if err != nil {
		t.Fatalf("ParseLSAK: %v", err)
	}
	if id != 0xDEADBEEF || status != AckStored {
		t.Fatalf("id/status = %x/%d", id, status)
	}
	if AckName(status) != "stored" || AckName(NackCorrupt) != "nack-corrupt" {
		t.Fatal("AckName mapping broken")
	}

	if _, _, err := ParseLSAK(frame[:10]); err == nil {
		t.Fatal("short LSAK accepted")
	}
	frame[0] = 'X'
	if _, _, err := ParseLSAK(frame); err == nil {
		t.Fatal("bad LSAK magic accepted")
	}
}

func TestEncodeTLVsLayout(t *testing.T) {
	out := EncodeTLVs([]TLV{{Type: TLVAssert, Value: []byte("abc")}})
	if len(out) != 4+3 {
		t.Fatalf("length = %d", len(out))
	}
	if binary.LittleEndian.Uint16(out[0:2]) != TLVAssert || binary.LittleEndian.Uint16(out[2:4]) != 3 {
		t.Fatal("TLV header wrong")
	}
	if !bytes.Equal(out[4:], []byte("abc")) {
		t.Fatal("TLV value wrong")
	}
}
