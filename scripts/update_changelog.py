#!/usr/bin/env python3
"""Update CHANGELOG.md for a new release version.

Moves the notes under ``## [Unreleased]`` into a dated ``## [X.Y.Z]`` section,
verbatim — so the *style* of an entry is whatever the author wrote. To keep that
style consistent release after release, a persistent HTML-comment style guide
(see ``GUIDE``) lives at the top of the ``[Unreleased]`` section: it is left in
place when a release is cut, never carried into the dated section. Edit ``GUIDE``
to change the house style; it renders as nothing on GitHub.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Persistent authoring convention, kept under [Unreleased] across releases.
# Terse "bullet + brief why": what changed, then a short clause of rationale only
# where it earns its place. Matches the existing entries in CHANGELOG.md.
GUIDE = """<!--
Changelog style — terse "bullet + brief why":
- One bullet per user-facing change. State what changed; add a short clause of
  rationale only where it matters (breaking changes, gotchas, version bumps).
- Keep symbol/API names and version requirements; drop mechanism and backstory
  (the git history holds those). These notes are for a reader deciding whether
  to upgrade.
- Prefer one bullet with semicolon-joined clauses over nested sub-bullets.
- HARD CAP: three lines per bullet. If it needs more, it belongs in the commit
  message or the PR, not here.
- Group under ### Added / Changed / Fixed / Removed. Mark breaking changes
  **Breaking:**. See the released entries below for the target density.
-->"""

# Leading HTML comment at the start of the [Unreleased] body — the style guide.
_GUIDE_RE = re.compile(r"\A\s*<!--.*?-->\s*", re.S)


def _split_guide(body: str) -> tuple[str, str]:
    """Split a leading HTML-comment guide off the [Unreleased] body.

    Returns ``(guide, notes)``: the guide comment (verbatim, or "" if none) and
    the remaining release notes stripped of surrounding whitespace.
    """
    match = _GUIDE_RE.match(body)
    if not match:
        return "", body.strip()
    return match.group(0).strip(), body[match.end() :].strip()


def _unreleased_block(ver: str, date: str, guide: str, notes: str) -> str:
    """The replacement text: a fresh [Unreleased] (guide preserved) followed by
    the new dated section carrying the notes."""
    section = (
        f"## [{ver}] - {date}\n\n{notes}\n\n"
        if notes
        else f"## [{ver}] - {date}\n\n### Changed\n\n- Release {ver}\n\n"
    )
    unreleased = "## [Unreleased]\n\n"
    if guide:
        unreleased += f"{guide}\n\n"
    return unreleased + section


def update_changelog_text(text: str, ver: str, date: str) -> str:
    if re.search(rf"^## \[{re.escape(ver)}\]", text, re.M):
        return text

    unreleased = re.search(r"^## \[Unreleased\]\s*\n(.*?)(?=^## \[|\Z)", text, re.M | re.S)
    if unreleased:
        guide, notes = _split_guide(unreleased.group(1))
        block = _unreleased_block(ver, date, guide, notes)
        return re.sub(
            r"^## \[Unreleased\]\s*\n.*?(?=^## \[|\Z)",
            lambda _match: block,
            text,
            count=1,
            flags=re.M | re.S,
        )

    section = f"{_unreleased_block(ver, date, GUIDE, '')}"
    first_heading = re.search(r"^## \[", text, re.M)
    if first_heading:
        pos = first_heading.start()
        return text[:pos] + section + text[pos:]
    return text.rstrip() + "\n\n" + section


def update_changelog_file(path: Path, ver: str, date: str) -> None:
    if not path.exists():
        path.write_text(
            "# Changelog\n\n"
            "All notable changes to this project will be documented in this file.\n\n"
            "The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),\n"
            "and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).\n\n"
            f"{_unreleased_block(ver, date, GUIDE, '')}"
        )
        return

    path.write_text(update_changelog_text(path.read_text(), ver, date))


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: update_changelog.py VERSION YYYY-MM-DD")
    update_changelog_file(Path("CHANGELOG.md"), sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()
