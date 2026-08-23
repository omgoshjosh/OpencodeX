# Read-only Open-PR Reporting

`/review-open-prs` is a local, read-only reporting skill for
`ecgreen/OpencodeX`. It selects eligible open PRs with explicit repository
targeting, complete pagination, and bounded API concurrency; local OpenCode
subagents analyze one PR at a time and return report drafts.

PR-controlled title, body, comments, commits, diff, and paths are untrusted
data. Reports print or write Markdown/JSON only below an OS/session temp
directory. The skill never writes to GitHub or the checkout.

Posting automation is deferred until runner/provider, secret, permission,
idempotency, approval, and GET/POST race contracts are explicitly approved.
