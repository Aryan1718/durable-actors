# Terse engineering conventions

Rules for working in this repo. Keep changes minimal and idiomatic.

## Core rules

1. **Minimize comments.** Add one only when a choice is non-obvious, odd, or a deliberate compromise whose rationale must remain beside the code.

2. **Prefer a library** over building it yourself for common problems.

3. **Use dependency injection at effectful boundaries.** Code against a narrow interface when an implementation must be replaceable, and inject dependencies through constructors. Do not add interfaces around pure helpers.

4. **Follow the step-down rule.** Present public orchestration first, followed by progressively lower-level helpers in call order.

5. **Keep methods and functions short.** Split work into named operations when a function mixes responsibilities or abstraction levels.

6. **Follow SOLID principles**
