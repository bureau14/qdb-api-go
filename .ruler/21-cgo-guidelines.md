## CGO

This project relies on CGO for interaction with the QuasarDB C API.

In CGO, balance performance, correctness, and memory safety:
 * Clearly indicate when unsafe CGO variants improve performance by avoiding copies or allocations.
 * Name such variants using the ...Unsafe suffix to explicitly indicate their risk.
 * Always strictly adhere to Go 1.23+ runtime CGO pointer-safety constraints:
  * Never pass Go-managed memory pointers (e.g., pointers to slices or structs allocated by Go) directly into C functions, use `runtime.Pinner` instead.
  * If memory cannot be pinned, allocate memory intended for direct use by C explicitly via approved Go→C allocation wrappers (such as qdbAllocBytes).
 * Always document:
  * Pointer lifetimes, memory ownership, and required release procedures.
  * Assumptions about type sizes, alignment, endianness, and binary representations, especially when directly copying between Go and C memory (e.g., float64 ↔ C.double).
 * Explicitly note if any assumption relies on IEEE-754 compliance or platform-specific ABI guarantees.
 * Provide robust, context-aware error handling, and detailed comments describing memory and performance trade-offs.
