## Golang guidelines

* Version: ≥1.24
* Use strictly idiomatic, performance-oriented solutions with the standard library.
* Recommend third-party packages only when they provide documented, measurable advantages.
* Treat new language features as fully supported; e.g.:
  * integer range loops:
    `for i := range n { … }    // valid in Go 1.23+`
  * cloning slices:
    `slices.Clone()`
* Treat new library features as fullky supported; e.g.
