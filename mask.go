// Copyright (c) 2025 QuasarDB SAS
// All rights reserved.
//
// Package qdb provides an API to a QuasarDB server.
package qdb

import (
	"encoding/binary"
	"math/bits"
)

// Mask is the bitset of a masked array: bit i is 1 when slot i holds a
// value and 0 when it is null. This is the opposite polarity of numpy,
// where a set mask bit hides the value. Bits are packed LSB first into
// bytes, bit i in byte i/8 at position i%8, and that byte slice is the
// only storage. Bytes rather than 64-bit words because a byte layout has
// one meaning on every host: a word layout would present a different byte
// order on a big-endian machine, so a consumer handing Bytes to another
// process or library could not treat it as a portable bitmap. The query
// converter is the only writer, and a Mask is immutable once its column is
// built.
type Mask struct {
	bits []byte
	n    int
}

// newMask returns a mask of n slots with every bit clear (all null). The
// converter sets bits as it materialises non-null cells.
func newMask(n int) Mask {
	return Mask{bits: make([]byte, (n+7)/8), n: n}
}

// Len returns the number of slots.
func (m Mask) Len() int {
	return m.n
}

// IsValid reports whether slot i holds a value. The index is unchecked.
func (m Mask) IsValid(i int) bool {
	return m.bits[i>>3]>>(i&7)&1 == 1
}

// Bytes returns the packed bits, (Len()+7)/8 bytes, read-only: bit i of
// the mask is bit i%8 of byte i/8. Bits past Len() in the last byte are
// clear, so the slice can be compared or hashed as it is.
func (m Mask) Bytes() []byte {
	return m.bits
}

// NullCount returns the number of null slots.
func (m Mask) NullCount() int {
	// Padding bits past n in the last byte are never set because set is
	// the only writer and it is only called with i < n, so every byte can
	// be popcounted without masking the tail.
	valid := 0
	b := m.bits
	// Eight bytes per popcount: the little-endian read is a single load
	// on x86-64 and aarch64, and a bit count does not depend on the byte
	// order, so the result is right on any host.
	for ; len(b) >= 8; b = b[8:] {
		valid += bits.OnesCount64(binary.LittleEndian.Uint64(b))
	}
	for _, x := range b {
		valid += bits.OnesCount8(x)
	}

	return m.n - valid
}

// AllValid reports whether no slot is null. True for an empty mask.
func (m Mask) AllValid() bool {
	return m.NullCount() == 0
}

// AllNull reports whether every slot is null. True for an empty mask.
func (m Mask) AllNull() bool {
	for _, b := range m.bits {
		if b != 0 {
			return false
		}
	}

	return true
}

// set marks slot i as holding a value. Requires 0 <= i < Len().
func (m *Mask) set(i int) {
	m.bits[i>>3] |= 1 << (i & 7)
}
