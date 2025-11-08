# Codex Agent Reference

Codex CLI is already configured for this repo. Follow these guardrails while operating:

- **Planning** – when a task benefits from planning, drop a Markdown plan in `.agents/plans/` and update it as you progress. Skip formal plans for trivial edits.
- **Editing** – prefer `apply_patch` for manual changes. Use scripts or generators only when patching becomes impractical (e.g., large rewrites, formatters).
- **Validation** – default test commands live in Pipenv:
  - `pipenv run test` (unit tests)
  - `pipenv run integrations` (local DynamoDB integration suite)
  - `pipenv run lint`, `pipenv run mypy`, `pipenv run coverage` when quality gates are required
- **Encoding** – keep files ASCII unless the target already contains non-ASCII or the request mandates otherwise.
- **Local diffs** – do not revert user changes you didn’t author. If unexpected edits block your work, stop and ask for guidance.
- **SNS context** – adapter-level `sns_attributes` act as defaults; per-call attributes override/extend them and every publish auto-adds `operation`. Ensure tests touching `BaseAdapter` cover that merge behavior.

Consult `AGENTS.md` for task-specific rituals and the README for library usage expectations.
