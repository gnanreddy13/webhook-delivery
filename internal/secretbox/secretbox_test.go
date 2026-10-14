package secretbox

import (
	"strings"
	"testing"
)

var (
	keyA = strings.Repeat("ab", 32)
	keyB = strings.Repeat("cd", 32)
)

func TestSealOpenAndRotation(t *testing.T) {
	a, err := New(keyA, "")
	if err != nil {
		t.Fatal(err)
	}
	sealed, _ := a.Seal("whsec_123")
	sealed2, _ := a.Seal("whsec_123")
	if !IsSealed(sealed) || strings.Contains(sealed, "whsec_123") || sealed == sealed2 {
		t.Fatalf("bad ciphertext %q (must be prefixed, opaque and randomized)", sealed)
	}
	if p, prev, err := a.Open(sealed); err != nil || p != "whsec_123" || prev {
		t.Fatalf("open: %q %v %v", p, prev, err)
	}

	// Rotate: B is current, A is previous.
	b, _ := New(keyB, keyA)
	if p, prev, err := b.Open(sealed); err != nil || p != "whsec_123" || !prev {
		t.Fatalf("open with previous key: %q %v %v", p, prev, err)
	}
	// Without the previous key the old value can't be read.
	bOnly, _ := New(keyB, "")
	if _, _, err := bOnly.Open(sealed); err == nil {
		t.Fatal("expected failure with wrong key")
	}
	// Tampering is detected.
	tampered := sealed[:len(sealed)-2] + "AA"
	if _, _, err := a.Open(tampered); err == nil {
		t.Fatal("expected failure on tampered ciphertext")
	}
	if _, _, err := a.Open("whsec_plain"); err == nil {
		t.Fatal("expected failure on plaintext value")
	}
}

func TestNewRejectsBadKeys(t *testing.T) {
	for _, k := range []string{"", "abc", strings.Repeat("ab", 16), strings.Repeat("zz", 32)} {
		if _, err := New(k, ""); err == nil {
			t.Errorf("key %q should be rejected", k)
		}
	}
}
