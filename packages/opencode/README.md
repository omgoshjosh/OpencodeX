# js

To install dependencies:

```bash
bun install
```

To run:

```bash
bun run index.ts
```

This project was created using `bun init` in bun v1.2.12. [Bun](https://bun.sh) is a fast all-in-one JavaScript runtime.

## Auth writers

`auth.json` service writers cooperate through `${authFile}.lock`. `Auth.set` and
`Auth.remove` acquire that exclusive lock, reread a whole valid snapshot, and
atomically replace the file. Other writers should use those service methods.

Arbitrary external atomic renames do not participate in the lock and are
last-rename-wins. They remain supported for credential hot reload, but cannot
receive compare-and-swap/no-lost-update guarantees.
