# Read-only Open-PR Reporting Plan

Maintain deterministic, read-only selection and bounded paginated GitHub reads.
Dispatch analysis through local OpenCode subagents, surface analyzer failure,
and emit reports only to OS/session temporary storage. Do not add GitHub write
permissions, workflow automation, markers, locks, or posting behavior.
