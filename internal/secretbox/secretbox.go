// Package secretbox encrypts endpoint signing secrets at rest with
// AES-256-GCM. Stored values look like "v1:<base64(nonce || ciphertext)>".
package secretbox

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const prefix = "v1:"

// Box seals with the current key and opens with the current key or, during
// a key rotation, the previous one.
type Box struct {
	current  cipher.AEAD
	previous cipher.AEAD // nil when not rotating
}

// New takes 64-hex-character (32-byte) keys. previousHex may be empty.
func New(currentHex, previousHex string) (*Box, error) {
	cur, err := newAEAD(currentHex)
	if err != nil {
		return nil, fmt.Errorf("SECRET_ENCRYPTION_KEY: %w", err)
	}
	b := &Box{current: cur}
	if previousHex != "" {
		if b.previous, err = newAEAD(previousHex); err != nil {
			return nil, fmt.Errorf("SECRET_ENCRYPTION_KEY_PREVIOUS: %w", err)
		}
	}
	return b, nil
}

func newAEAD(keyHex string) (cipher.AEAD, error) {
	key, err := hex.DecodeString(keyHex)
	if err != nil || len(key) != 32 {
		return nil, errors.New("must be 64 hex characters (32 bytes), e.g. openssl rand -hex 32")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// IsSealed reports whether a stored value is in the encrypted format.
func IsSealed(stored string) bool {
	return strings.HasPrefix(stored, prefix)
}

func (b *Box) Seal(plaintext string) (string, error) {
	nonce := make([]byte, b.current.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	out := b.current.Seal(nonce, nonce, []byte(plaintext), nil)
	return prefix + base64.StdEncoding.EncodeToString(out), nil
}

// Open decrypts a stored value. usedPrevious is true if only the previous
// key could open it, meaning it should be re-sealed with the current key.
func (b *Box) Open(stored string) (plaintext string, usedPrevious bool, err error) {
	if !IsSealed(stored) {
		return "", false, errors.New("secret is not encrypted")
	}
	raw, err := base64.StdEncoding.DecodeString(stored[len(prefix):])
	if err != nil {
		return "", false, err
	}
	if p, err := open(b.current, raw); err == nil {
		return p, false, nil
	}
	if b.previous != nil {
		if p, err := open(b.previous, raw); err == nil {
			return p, true, nil
		}
	}
	return "", false, errors.New("cannot decrypt secret with the configured key(s)")
}

func open(aead cipher.AEAD, raw []byte) (string, error) {
	n := aead.NonceSize()
	if len(raw) < n {
		return "", errors.New("ciphertext too short")
	}
	p, err := aead.Open(nil, raw[:n], raw[n:], nil)
	return string(p), err
}
