Check that all documentation and version references are consistent before a drt release.

## Steps

1. **Version consistency** — verify the same version string in:
   - `pyproject.toml` (project.version)
   - `.claude-plugin/marketplace.json`
   - `.claude-plugin/plugin.json`
   - `skills/drt/.claude-plugin/plugin.json`

2. **CHANGELOG** — verify there is an entry for the current version with today's date

3. **README.md** — verify:
   - Roadmap table: current version has ✅
   - Connectors table: new destinations/sources are listed with correct status
   - Quickstart section is up to date

4. **CLAUDE.md** — verify:
   - Current Status reflects the latest version
   - Sources/Destinations lists are complete
   - Roadmap Reference is current

5. **SECURITY.md** — verify current version is in Supported Versions

6. **docs/llm/CONTEXT.md** — verify:
   - Current version is correct
   - Destinations table includes all destinations

7. **docs/llm/API_REFERENCE.md** — verify:
   - All destination types have config examples
   - All destination types have complete examples

8. **Skills** — verify:
   - `.claude/commands/drt-create-sync.md` lists all destinations
   - `skills/drt/skills/drt-create-sync/SKILL.md` lists all destinations

9. **dagster-drt dependency** — verify:
   - `integrations/dagster-drt/pyproject.toml` has `drt-core>=` matching or exceeding the version being released
   - If drt-core has breaking changes, dagster-drt tests still pass

10. **CI** — verify all tests pass: `make test && make lint`

11. **GitHub Milestone** — verify:
    - All milestone issues are closed or moved
    - No open PRs blocking the release

12. **GitHub Release — drt-core** — create (or verify exists):
    - `gh release create v{VERSION}` with title `v{VERSION}`
    - Release notes: "What's New" sections matching CHANGELOG entry
    - Mark as `--latest` for the primary release
    - Include `Full Changelog` compare link

13. **GitHub Release — dagster-drt** (if version bumped) — create (or verify exists):
    - `gh release create dagster-drt-v{VERSION} --latest=false` with title `dagster-drt v{VERSION}`
    - Release notes: features, requirements, PyPI link
    - **MUST pass `--latest=false`** (gh defaults to auto-detect by date, which will steal Latest from drt-core)

14. **Verify Latest flag** — after all releases are created:
    - `gh release list --limit 5` — confirm drt-core `v{VERSION}` shows `Latest`, dagster-drt does not
    - If wrong: `gh release edit dagster-drt-v{VERSION} --latest=false && gh release edit v{VERSION} --latest`

Report any inconsistencies found and suggest fixes.