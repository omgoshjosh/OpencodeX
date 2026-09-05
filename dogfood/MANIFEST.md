# Dogfood Stack2 Reconstruction Manifest

This is the canonical reconstruction record for OpenCodeX PR #38. It is
deliberately hash-first: branch names are recorded only where Git history
identifies them. Do not infer a branch name from a commit subject.

## Immutable Anchors

- Repository: OpenCodeX fork; checkout `/Users/josh/agents/worktrees/dogfood-stack`.
- Base at manifest creation: PR #31 / `chore/upstream-pin-review-fixes` at
  `b5a5500801dde38a9d411e52c36623ae2216493d`.
- Target: `fork/dogfood/stack2`. Tip at manifest creation was
  `7499962fc8c14819a70e6ff10ece13bd59f9570a`, tree
  `9c76c88630783dd3af9b7a511238fc9c20cdb2dd`. Superseded — see
  "Rebuild 2026-09-05" below for the current anchors.
- Expected tip tree is not a mutable constant. Verify it with:
  `git show -s --format='tip=%H%n tree=%T%n subject=%s' <tip>`.
- Reconstruct from the recorded base, not from `main`, a moving remote-tracking
  ref, or an unpinned branch.

## Rebuild 2026-09-05

`fork/dogfood/stack2` was rebuilt onto the advanced PR #31 head. Anchors:

- New base: PR #31 head `7d6e2369eeea317631c3cddccf9fe5d2cdfc7ebc`
  (`test(snapshot): give the 101-file revert test Windows headroom (#31)`),
  which supersedes `b5a5500801`.
- Old tip: `ecdbe69d22b1e85275cf40818c7c15b459d27fcf`.
- New verified tip: `c7c7df4773a095287e221c59b15f5fcf0bf97dff`, tree
  `6e418daa0f5cc8045f18bec173cc6c00b403a48d`.

The new tip's tree differs from the old tip's tree by exactly the base delta —
i.e. only the PR #31 commits between `b5a5500801` and `7d6e2369ee`:

```
git diff --stat ecdbe69d22 HEAD
 packages/opencode/test/session/tools.test.ts     | 13 +++++++++++--
 packages/opencode/test/snapshot/snapshot.test.ts |  7 +++++++
```

The stack's own content is byte-identical across the rebase. Verified with:

```
diff <(git diff b5a5500801 ecdbe69d22 | grep -v '^index ') \
     <(git diff 7d6e2369ee HEAD     | grep -v '^index ')
```

The only difference that command reports is a single *removed* line inside
`packages/opencode/test/session/durable-execution.test.ts`
(`expect(row?.status.background).toBeUndefined()` vs. the cast-guarded
`expect((row?.status as { background?: unknown })?.background).toBeUndefined()`).
That base hunk is superseded: the stack rewrites the same assertion later in
its own lineage, so both parents converge on the same final text. No stack
commit was dropped, added, or edited by the rebuild.

## Groups since Task 8

Ordered first-parent sequence from the new base, taken from
`git log --reverse --first-parent 7d6e2369ee..HEAD`, listed after the Task 8
block (which ends at `6c6ca2210aac4a53e489eaa6031ace97fa4b28ba`). Groups are
interleaved in history; the order below is the true first-parent order.

1. Task 5 late fold — `27853b2fef526b6abaec8ca52ba1473af8ff6bd9`.
2. Task 6 late fold — `c678090a25cef239e29fdcfe3d60c0e87a3e973d`.
3. Task 9, descendant process cleanup — `a68f0ae98b2ae005f462ede2b5f83951157d30e9`,
   `5e1d9b3e6337dbfbcbf50d169e514b97efc4c7a1`,
   `80832672e1538e853d3af0744af90c1007666fae`.
4. Task 7 late fold — `23abb8ecfbe205fbe05005654d48885ef0547dce0`.
5. Task 10, hung Claude query creation — `0bbc257fd34a626e27a35c28c0ebbeca46b10308`.
6. Task 4 (part 1), no-op transcript recovery — `b1b80867dd51cc7bf8cc7c58a31184b1378621bb`.
7. Task 11, upstream security salvage — `1cf2d41acb555f3b3baba4b6a5b47d0d1e197254`,
   then (after the Task 12 commit below) `dd7d5204fcbc0c49f5e0c836fa98ee7896dd9bbb`.
8. Task 12, idempotent durable cancellation — `d099f8cf3bc02d9733bc5b8f6ac14b4fa18c04c4`.
9. Task 4 (part 2), restart readiness excludes the caller —
   `72fe2bb9455ea3ef214eae831a6b7c40edd25511`.
10. CI-retrigger empty (no files changed) — `a803ed477706980aee34898dc25d6544f757a3ad`
    (`chore: retrigger CI after unit(linux) flake`).
11. Local lockstep build/deploy fold — `9d78a460bd12b893c4fb071a735b5fa69fc9cbb1`,
    `5ed39cf97819bbbaf5c0599a68b60d9d339ef15f`. These close
    https://github.com/ecgreen/OpencodeX/issues/42; no commit in this range
    references issue #34.
12. Task 13 (part 1), durable background task reports —
    `798de785d95b3888a5f362228b2ef5e9565f5b94`.
13. dio.6 fold, server-authoritative mark-unread —
    `70733c165af5465396915fe2dfff8b4587f0537a`,
    `a11db0f36670914430ae7b61be42f7e2cb5bc6e8`,
    `0dd1df4c66d436a1aaf155f733cd7161de72f83e`.
14. Task 13 (part 2), LLM stream idle watchdog —
    `5275ea9f5b8170eb9eb24a5c1fcbde6b4ce5c417`,
    `826b5ecdd1523ccbcb5512e609b40c7a915f1f4c`,
    `4e2a96778f724ce49814a75cc33fb8bf4a2ef62f`.
15. CI-retrigger empty (no files changed) — `c7c7df4773a095287e221c59b15f5fcf0bf97dff`
    (`chore: retrigger CI after unit(windows) pwsh flake`), the current tip.

## Replay Order

Apply the following immutable first-parent sequence in order. Each range is
inclusive of the listed commits and has its parent recorded by Git; verify
before applying with `git cat-file -e <sha>^{commit}`.

1. Core reliability and retention lineage: `b5a5500..c13f573` (canonical
   stack2 branch tip `c13f5739378a2de57609486c9a98602c283ab183`). Preserve the
   commits in history; do not replace this with a fresh upstream merge.
2. Task 7 control branch: `opencode-task7-control` at
   `cc85372fd27fd276f0f5af4ec434ee5f6f06bc06`. Its relevant control/test
   commit is `cc85372`; use the hash, not a guessed source branch, for any
   missing intermediate mapping.
3. Task 7 corrected k5j/dio sequence, oldest to newest:
   `7495d3a0345add2b9b1663af367626690cc38c28`,
   `944724bac74118c2bd3a1da12c12c2897415a2b0`,
   `0d7935c9e6ef0de4d4581f7f906cd8448d3c3fb7`,
   `f7c08a5e11214e94a5e56a19a5ebe8e4aae25b8c`,
   `6f790cb105cc2d8193af6a2a278ab8204b187c86`,
   `3d86b0ea5cc682fe553ab1a892a70307c387df60`.
   The identifiable prep branch is `opencode-dogfood-stack2-task7-prep` at
   `6f790cb`; do not invent names for the other commits.
4. k5j corrected continuation, oldest to newest:
   `ae55bbb5457e2e3ae206a8d18c3d12730ef24347`, then
   `3368801ffb734bcf231af66e16ffe3b74c5f621b`, then
   `c13f5739378a2de57609486c9a98602c283ab183` as the stack2 stabilization
   point. `3d86b0e` is the generated-file hook correction immediately before
   this continuation and must not be dropped.
5. Task 8, in exact order through the requested tip:
   `a02a9f8f502f1cf55d6f3160de9a3cdac5b14796`,
   `3582021c025e84c728f101576756a17f9749e9b0`,
   `8d49b629230fe455225fe56b124666a5a911a724`,
   `34f8c340eb9e7bdd5094b54269e5f7c69e46f05d`,
   `e986f957d7839fd7c4f0ea687c6d91558de86a86`,
   `0f277d7af22f60194511d4ff99e56b6a45daf1d0`,
   `f81b542dc3e6bdce73bfbe31708eeac629bf743f`,
   `7499962fc8c14819a70e6ff10ece13bd59f9570a`.

If a source branch cannot be proven by `git show-ref`, use its immutable
commit range/SHA and stop. Never substitute a similarly named branch.

## Explicit Omissions

Do not replay these nine obsolete daily-sync release commits into stack2:

`a9cac91d60660abac2cfe29afbdd466f60e765be`,
`ad0bb6d9a3e779def694adc093a811e86a529df0`,
`55f984126cbe26920e532d1e2b09cb16482cb451`,
`2f36ffe35d569bd0fb1ae6e22f4a859ac08177e0`,
`19db518e0a851160cc77230320125563f4cb117f`,
`755ebdb94ee755a9d5691e47af2c16f56696996e`,
`8e0f1c253b6b7292b419505af849d06747c0e049`,
`05028334b27b97c227f22bda50a53c8932f9a93c`,
`51f86c853791c41656fb0adcf9413291e4996b87`.

Do not replay upstream `main` or unrelated desktop/TUI work. Resolve only the
conflicts represented by the pinned stack commits. The verified named
resolution commits are `f883323aaa4ce3327a8c22144485215cf3109c02`,
`f9014d79299ef0f3090417cde746c1dbf5c275c6`, and
`16cbbb97e911c8333dfc217ddf101efd4ec23e6c`; retain them in order where their
parents are in the selected lineage. No merge commit is a license to absorb
the moving upstream branch.

## Generation

From the repository root, after the source/API changes and before validation:

```bash
cd /Users/josh/agents/worktrees/dogfood-stack
bun --cwd packages/core run migration
./packages/sdk/js/script/build.ts
```

The core migration command must produce the checked-in migration journal,
snapshot, and `packages/core/src/database/migration.gen.ts`; the SDK command
must produce the checked-in JS SDK generated files. Inspect `git diff` and do
not hand-edit generated output. Run both commands a second time; the second
run must be clean (`git diff --exit-code`).

## Human Followups

- Confirm the PR #38 review/acceptance decision for the Task 7 and Task 8
  behavior; this manifest does not declare deployment approval.
- Confirm migration compatibility and production backup/rollback ownership
  before any cutover.
- Confirm the final source-to-branch mapping for any unnamed commit before
  publishing a new branch; the immutable SHA remains authoritative.

## Quality Gates

```bash
git diff --check
git fsck --no-reflogs --full
bun run typecheck
bun run lint:ci
bun run surface:audit
cd packages/core && bun test
cd ../sdk/js && bun test
cd ../../.. && git diff --exit-code
```

Record failures and environment limitations; do not weaken a gate to obtain a
green result.

## Branch, Mirror, and Cutover Policy

- `fork/main` is an immutable mirror for this exercise. Never force it, merge
  into it, or use it as a rebuild target.
- Rebuild only `fork/dogfood/stack2`, and only after announcing the rebuild
  with the old tip, new base, ordered SHA list, and expected verification
  commands. If updating the remote branch is authorized, use
  `git push --force-with-lease=<old-tip>:refs/heads/dogfood/stack2` and verify
  the remote SHA immediately. Never use plain `--force`.
- Delete stale dogfood refs only after containment: stop deployments, preserve
  this manifest and the old tip, verify no active worktree/CI/deployment points
  at the ref, and record the exact refs/SHA inventory. Deletion is cleanup, not
  conflict resolution.
- Deploy/cut over only from the verified pinned tip after all gates pass.
  Announce artifact SHA, migration status, health checks, owner, and rollback
  target; deploy once, observe, then cut over. Do not run migrations or deploy
  from a dirty tree, a moving branch, or an unverified generated second run.
