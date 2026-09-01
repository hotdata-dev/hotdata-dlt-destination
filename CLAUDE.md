# Working in this repo

## CHANGELOG.md

**Keep entries short.** Three lines per bullet, hard cap. One bullet per
user-facing change, grouped under `### Added / Changed / Fixed / Removed`, and
prefer semicolon-joined clauses over sub-bullets.

Write for someone deciding whether to upgrade: what changed, and a short clause
of why only where it matters (breaking changes, gotchas, version bumps). Keep
symbol and API names; drop mechanism and backstory. If a bullet wants more room,
that detail belongs in the commit message or the PR body — both are one click
away and neither is read by someone scanning releases.

Mark breaking changes **Breaking:**. The style note at the top of the file is the
same rule; the released entries below it show the target density.
