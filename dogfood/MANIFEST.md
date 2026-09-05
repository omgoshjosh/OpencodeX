# Dogfood Stack2 Reconstruction Manifest

This is the canonical reconstruction record for OpenCodeX PR #38. It is
deliberately hash-first: branch names are recorded only where Git history
identifies them. Do not infer a branch name from a commit subject.

## Immutable Anchors

- Repository: OpenCodeX fork; checkout `/Users/josh/agents/worktrees/dogfood-stack`.
- Base: PR #31 / `chore/upstream-pin-review-fixes` at
  `b5a5500801dde38a9d411e52c36623ae2216493d`.
- Target: `fork/dogfood/stack2`; current verified tip
  `7499962fc8c14819a70e6ff10ece13bd59f9570a`.
- Expected tip tree is not a mutable constant. Verify it with:
  `git show -s --format='tip=%H%n tree=%T%n subject=%s' 7499962fc8`.
  At manifest creation the output tree was `9c76c88630783dd3af9b7a511238fc9c20cdb2dd`.
- Reconstruct from the base, not from `main`, a moving remote-tracking ref, or
  an unpinned branch: `git checkout -B dogfood/stack2 b5a5500801dde38a9d411e52c36623ae2216493d`.

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
   `0d7935c9e6ef0de4d4581f7f906cd8448d3c3fb7`,
   `f7c08a5e11214e94a5e56a19a5ebe8e4aae25b8c`,
   `6f790cb105cc2d8193af6a2a278ab8204b187c86`,
   `3d86b0ea5cc682fe553ab1a892a70307c387df60`,
   `7495d3a0345add2b9b1663af367626690cc38c28`,
   `944724bac74118c2bd3a1da12c12c2897415a2b0`.
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
