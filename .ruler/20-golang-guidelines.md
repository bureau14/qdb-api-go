## Golang guidelines

* Version: ≥1.24
* Use strictly idiomatic, performance-oriented solutions with the standard library.
* Recommend third-party packages only when they provide documented, measurable advantages.
* Treat new language features as fully supported; e.g.:
  * integer range loops:
    `for i := range n { … }    // valid in Go 1.23+`
  * cloning slices:
    `slices.Clone()`
* Treat new library features as fully supported; e.g.

## Golang test guidelines
* use Go’s testing + testify (assert/require)
* isolate all tests; no shared state
* ALWAYS generate random aliases/tags/table names – never hard-code them
  – use generateAlias(), generateTags() or equivalent, or create a helper if needed
* add/extend helpers in test_utils.go where useful
