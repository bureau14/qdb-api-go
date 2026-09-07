// Copyright (c) 2025 QuasarDB SAS
// All rights reserved.
//
// Package qdb provides an API to a QuasarDB server.
package qdb

import "math/bits"

// Bitmap is a validity mask: bit i is 1 when slot i holds a value and 0
// when it is null. Bits are packed LSB first into 64-bit words, bit i in
// word i/64 at position i%64. The query converter builds one per result
// column; it is the only writer, and a Bitmap is immutable once its column
// is built.
type Bitmap struct {
	bits []uint64
	n    int
}

// newBitmap returns a mask of n slots with every bit clear (all null).
// The converter sets bits as it materialises non-null cells.
func newBitmap(n int) Bitmap {
	return Bitmap{bits: make([]uint64, (n+63)/64), n: n}
}

// Len returns the number of slots.
func (b Bitmap) Len() int {
	return b.n
}

// IsValid reports whether slot i holds a value. The index is unchecked.
func (b Bitmap) IsValid(i int) bool {
	return b.bits[i>>6]>>(i&63)&1 == 1
}

// NullCount returns the number of null slots.
func (b Bitmap) NullCount() int {
	// Padding bits past n in the last word are never set because set is
	// the only writer and it is only called with i < n, so every word can
	// be popcounted without masking the tail.
	valid := 0
	for _, w := range b.bits {
		valid += bits.OnesCount64(w)
	}

	return b.n - valid
}

// AllValid reports whether no slot is null. True for an empty bitmap.
func (b Bitmap) AllValid() bool {
	return b.NullCount() == 0
}

// AllNull reports whether every slot is null. True for an empty bitmap.
func (b Bitmap) AllNull() bool {
	for _, w := range b.bits {
		if w != 0 {
			return false
		}
	}

	return true
}

// set marks slot i as holding a value. Requires 0 <= i < Len().
func (b *Bitmap) set(i int) {
	b.bits[i>>6] |= 1 << (i & 63)
}
