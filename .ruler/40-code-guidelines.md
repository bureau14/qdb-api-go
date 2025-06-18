## Performance

Performance is critical. Code will run trillions of times per day in a high-performance timeseries database.

- Avoid copies of arrays, but instead prefer zero-copy pointer-passing where possible
- Explicitly document trade-offs (complexity, maintainability, readability vs performance).
- Recommend explicitly when each approach should be chosen.
- Provide concise, actionable examples of recommended performance techniques.

## Function Documentation Style

When documenting functions, closely follow the commenting approach used at the bottom of `utils.go` and the generators in `test_utils.go`.

* Use concise, high-signal comments that explain *why* and *how* rather than repeating *what* the code already shows.
* Use _only_ single-line comments starting with `//`.  Block comments (`/* … */`) are **prohibited**.
- For non-trivial functions, include bullet lists for key sections, each prefixed with `//`:
  - `// Decision rationale:` followed by short bullets describing choices made.
  - `// Key assumptions:` bullets stating the required preconditions or invariants.
  - `// Performance trade-offs:` bullets outlining costs versus benefits.
- For non-trivial functions, provide an inline usage example with each line starting with `//`. Avoid fenced code blocks so the comment format mirrors production code.
- Avoid trivial comments. Focus on explaining design choices, trade-offs and assumptions.
- Optimize for clarity and LLM readability. Never use emojis.

## Helper Function Guidelines

Create small, composable helper functions where practical. Prefer reusing existing helpers from `utils.go` and `test_utils.go` instead of duplicating logic.
