// Copyright (c) 2025 QuasarDB SAS
// All rights reserved.
//
// Package qdb provides an API to a QuasarDB server.
package qdb

import "math/bits"

// Mask is the bitset of a masked array: bit i is 1 when slot i holds a
// value and 0 when it is null. This is the opposite polarity of numpy,
// where a set mask bit hides the value. Bits are packed LSB first into
// 64-bit words, bit i in word i/64 at position i%64. The query converter is
// the only writer, and a Mask is immutable once its column is built.
type Mask struct {
	bits []uint64
	n    int
}

// newMask returns a mask of n slots with every bit clear (all null). The
// converter sets bits as it materialises non-null cells.
func newMask(n int) Mask {
	return Mask{bits: make([]uint64, (n+63)/64), n: n}
}

// Len returns the number of slots.
func (m Mask) Len() int {
	return m.n
}

// IsValid reports whether slot i holds a value. The index is unchecked.
func (m Mask) IsValid(i int) bool {
	return m.bits[i>>6]>>(i&63)&1 == 1
}

// NullCount returns the number of null slots.
func (m Mask) NullCount() int {
	// Padding bits past n in the last word are never set because set is
	// the only writer and it is only called with i < n, so every word can
	// be popcounted without masking the tail.
	valid := 0
	for _, w := range m.bits {
		valid += bits.OnesCount64(w)
	}

	return m.n - valid
}

// AllValid reports whether no slot is null. True for an empty mask.
func (m Mask) AllValid() bool {
	return m.NullCount() == 0
}

// AllNull reports whether every slot is null. True for an empty mask.
func (m Mask) AllNull() bool {
	for _, w := range m.bits {
		if w != 0 {
			return false
		}
	}

	return true
}

// set marks slot i as holding a value. Requires 0 <= i < Len().
func (m *Mask) set(i int) {
	m.bits[i>>6] |= 1 << (i & 63)
}
