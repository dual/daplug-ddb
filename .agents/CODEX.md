# Codex Agent Reference

This repository is configured for Codex CLI agents. Keep these norms in mind while you work:

- **Plans live in `.agents/plans/`** – create a Markdown plan file there when the task calls for planning, and refresh it as the work evolves.
- **Prefer `apply_patch` for edits** – use it for manual changes to existing files. Reserve other techniques for generated outputs or large transforms.
- **Lean on existing tooling** – run `pipenv run test`, `pipenv run integrations`, `pipenv run mypy`, and `pipenv run lint` to validate changes when relevant.
- **Maintain ASCII unless necessary** – only introduce non-ASCII characters if the file already uses them or the user explicitly requests it.
- **Preserve user changes** – never revert unrelated local modifications. If something unexpected appears, pause and ask how to proceed.

Following these conventions keeps Codex agents aligned across contributions. Consult the README for project-specific details.
