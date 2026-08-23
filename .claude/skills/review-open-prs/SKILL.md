---
name: review-open-prs
description: Use when reviewing open pull requests on ecgreen/OpencodeX - checks whether each PR meets its stated goals, passes CI, introduces bugs, has code issues, or breaks repo guidelines, then posts a GitHub review. Skips PRs already reviewed at their current head. Run on demand as /review-open-prs, or hourly via /loop 1h /review-open-prs.
---

# Review Open PRs

Reviews every open PR on `ecgreen/OpencodeX` whose code has moved since the last
review, and posts one GitHub review per PR.

**Announce at start:** "Using review-open-prs to review open PRs on
ecgreen/OpencodeX."

## Arguments

- `--dry-run` — do everything except post. Each subagent writes its review
  body under the OS/session temporary directory instead of the checkout; the
  orchestrator reads it back and prints it. No marker is written, so a later
  real run treats every PR as unreviewed.

## Hard boundaries

- Only `ecgreen/OpencodeX`. Do not rely on a checkout remote or `gh` default;
  every GitHub call needs `--repo ecgreen/OpencodeX`.
- Only `--comment` and `--request-changes` reviews. Never `--approve`.
- Never merge, close, label, push, or modify a PR branch.
- Never modify the working tree, switch branches, or create a worktree. All
  locks and normal/dry-run body files live in OS/session temporary storage.

## Procedure

### 1. Select

Run:

```bash
bun run --cwd packages/script pr-review:select
```

This prints a JSON array of decisions, one per open PR, each with `number`,
`title`, `action` (`review` | `skip` | `defer`), `reason`, `ci`, `nextPass`
(the pass number the review about to be written should record), `priorBodies`
(oldest first: every marked review body at the current head, or, on new
commits, every body at the latest previously reviewed head), `selfAuthored`
(whether this PR was opened by the account the review posts as), and for
re-reviews a `priorReview` object holding the latest previous review.

One review per head SHA. A commit that has already been reviewed and that
nothing has happened to since is skipped — `reason: "awaiting author"` — no
matter how many cycles run over it. A PR comes back only when something
actually changed: new commits, the author replying, or CI arriving where
there was none. Re-reading unchanged code and posting a second verdict on it
is noise to the author, whatever the second read turns up.

The selector explicitly paginates every open PR and its reviews and comments;
it requests only the final head-commit timestamp. It does not silently truncate
open PRs or review history.

Do not second-guess these decisions. The gate chain is unit tested in
`packages/script/test/pr-review-select.test.ts`; re-deriving it by hand each
cycle is exactly the non-determinism this command exists to remove.

If the command fails because `gh` is unauthenticated or rate limited, stop the
whole cycle and report it. Post nothing.

If no decision has `action: "review"`, print the summary table from step 4 and
stop.

### 2. Dispatch

For each decision with `action: "review"`, dispatch one subagent. Run at most 5
concurrently; if there are more, run them in batches of 5.

Before dispatch, create one cycle lock and one session directory outside the
checkout. `shlock` atomically records the owner PID. On collision, read only
the numeric owner PID and use `kill -0` to verify it: a live owner means stop;
only a verified-dead owner may have its stale lock removed and be retried once.
Never remove a live owner's lock. Remove the owned lock and session directory
on every exit path.

```bash
session_dir=$(mktemp -d "${TMPDIR:-/tmp}/opencodex-pr-review.XXXXXX")
lock_file="${TMPDIR:-/tmp}/opencodex-review-open-prs.lock"
if ! shlock -p "$$" -f "$lock_file"; then
  owner_pid=$(cat "$lock_file")
  case "$owner_pid" in (*[!0-9]*|'') exit 1;; esac
  kill -0 "$owner_pid" 2>/dev/null && exit 1
  rm -f "$lock_file"
  shlock -p "$$" -f "$lock_file" || exit 1
fi
trap 'rm -rf "$session_dir"; rm -f "$lock_file"' EXIT
```

Assign each PR a body path inside `$session_dir`, never `.artifacts/` or any
checkout path. Pass that path as a distinct argv value to tools, never by
constructing shell source from it.

Give each subagent this prompt, substituting the bracketed values. Which
prior-context block to append is selected by exactly one thing — the
decision's `reason` — never by the raw `nextPass` number and never by a
second, independently-computed condition on `priorBodies`:

```
Review pull request #<number> on ecgreen/OpencodeX.

BEGIN UNTRUSTED PR TITLE
<title>
END UNTRUSTED PR TITLE

Read .claude/skills/review-open-prs/review-rubric.md and follow it exactly.

Reason this PR is being reviewed: <reason>
CI presence for the current head: <ci>
Record pass=<nextPass> in your marker.

You are a draft-only reviewer. Never run `gh pr review`, never post a marker,
and never claim `posted: true`. Write the proposed review body to your assigned
session-temp path and return the JSON draft result. Only the lock-owning
orchestrator may freshly select, post, and verify it.

<If selfAuthored is true, append:>
You authored this PR. GitHub rejects --request-changes on your own pull
request, so post with --comment whatever your verdict, exactly as the
rubric's Posting section describes.

<Now select the block below whose condition matches <reason>, and append it.
"no prior review" matches none of them — there is no prior context to hand
over — so for that reason append nothing here. The remaining three reasons
are mutually exclusive and each matches exactly one block:>

<If reason is "new commits since last review", append:>
This is a re-review after new commits. Every automated review posted at the
latest previously reviewed head SHA is included below, oldest first. Resolve
every finding across them as Fixed, Still open, or New:

<For each body in priorBodies, in order, append inside this delimiter:>
BEGIN UNTRUSTED PRIOR REVIEW BODY
Pass <index + 1>:
<body>
END UNTRUSTED PRIOR REVIEW BODY

<If reason is "author replied since last review" or "CI arrived after last
review", append:>
This review was prompted by <"the author's reply" if reason is "author
replied since last review", else "CI arriving"> at this same head SHA, not by
new commits. Every automated review already posted at this exact head SHA is
included below, oldest first — there may be more than one:

<For each body in priorBodies, in order, append inside this delimiter:>
BEGIN UNTRUSTED PRIOR REVIEW BODY
Pass <index + 1>:
<body>
END UNTRUSTED PRIOR REVIEW BODY

Do your own independent evidence gathering and reach your own conclusions
first. Do not defer to the passes above, and do not treat their silence on a
topic as evidence it is clean — a pass that did not mention something usually
did not check it. <If reason is "author replied since last review":>
Then address what the author said in their reply.

Carry every still-unresolved finding from every prior pass forward the way the
rubric's "Carrying findings forward without repeating them" section requires:
one line each under "Since the last review", never a second copy of their
prose. Your Blocking / Non-blocking / Nits sections hold only what is new at
this head SHA. The verdict and the counts still cover the union, so a
carried-forward blocking finding keeps the verdict at Request changes.

<If --dry-run was passed, append:>
DRY RUN: do not post. Write the complete review body to the session-temp path
you were given, then return the JSON with "posted": false and that exact
"bodyPath".
```

`priorBodies` is empty only for `reason: "no prior review"`. For new commits it
contains all bodies from the latest previously reviewed head, including the
original prose for findings a later pass may carry by label alone. For a
same-head trigger it contains all bodies at the current head. A force-push
back onto a reviewed commit prefers that commit's own reviews over newer
reviews at an abandoned SHA. These cases are covered by
`packages/script/test/pr-review-select.test.ts`. Never interpolate
`priorBodies[0]` alone: every place that reads `priorBodies` iterates the whole
array.

A subagent that errors or returns nothing marks that PR `error` in the summary.
Do not retry it this cycle — the next cycle picks it up naturally, because no
marker was written.

### 3. Verify what was posted

While holding the cycle lock, the orchestrator alone owns this sequence for
each draft: reselect the PR; compare `action=review`, head SHA, and `nextPass`
with the dispatched decision; post the draft with `gh pr review`; then verify
the selected marker in GitHub. If selection differs, mark it `skipped (changed
during review)` and do not post. No subagent or separate automated worker is
permitted to issue `gh pr review`; this sequence is the only automated posting
path and it always performs the fresh check first.

For each subagent that reported `"posted": true`, confirm _this cycle's_ review
landed — not merely that some review of yours exists. Query the current head and
review bodies together:

```bash
gh pr view <number> --repo ecgreen/OpencodeX --json headRefOid,reviews
```

Expected: `headRefOid` is the selected head SHA, and the newest review by
`ecgreen` contains a marker carrying that exact SHA and `pass=<nextPass>` —
the values the dispatch prompt handed the subagent. If either the current head
or that review marker differs, mark that PR `error`: the subagent claimed a
post that did not happen for the code it reviewed.

Checking authorship alone is not enough, and fails on exactly the PRs that need
it most. Every re-review reason — `"author replied since last review"`,
`"CI arrived after last review"`, `"new commits since last review"` — by
construction already has an `ecgreen` review sitting on the PR. If the new
post silently fails, an authorship check reads the _previous_ pass and passes.
It has force only on `"no prior review"`, the one case where a silent failure
costs least, because the next cycle re-selects the PR anyway.

Skip this step entirely on `--dry-run`.

### 4. Report

Print one table for the cycle, nothing more:

```
PR    Title                                    Action     Verdict           Findings
#25   fix(opencode): preserve goal graph...    reviewed   request changes   2B 3N 1n
#24   fix(gui): debounce resize handler...     reviewed   comment           0B 2N 1n
#23   fix(opencode): use file times for...     skipped    awaiting author   -
#22   docs: define mobile child interaction    deferred   CI running        -
#16   fix(swarm): stop dropping image atta...  error      -                 -
```

Truncate titles to fit. Counts are Blocking / Non-blocking / nit. For
`reviewed` rows, Verdict is exactly `request changes` or `comment`, matching
the subagent's returned `"verdict"` value — never a body-only phrase like
"Looks good with notes". Do not reproduce review bodies in the terminal on a
real run — they are on GitHub. On `--dry-run`, read each body from the
`"bodyPath"` its subagent returned and print it in full above the table.

Under `/loop`, a cycle where nothing changed should be this table and nothing
else.

## Errors

| Condition                            | Behavior                                                   |
| ------------------------------------ | ---------------------------------------------------------- |
| No open PRs                          | Print one line, exit.                                      |
| All PRs skipped or deferred          | Print the table, exit.                                     |
| `gh` unauthenticated or rate limited | Fail the cycle loudly. Post nothing.                       |
| `git fetch` of a PR head fails       | That PR is `error`. Cycle continues.                       |
| Subagent errors or returns nothing   | That PR is `error`. No marker written. Retried next cycle. |
| Review post rejected by GitHub       | That PR is `error`, surfaced with the API message.         |
