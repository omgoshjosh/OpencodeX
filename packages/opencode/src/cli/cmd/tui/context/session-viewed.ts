import type { OpencodeXSessionUiState } from "@opencode-ai/sdk/v2"

export function markViewedSessionUiState(
  sessionID: string,
  current: OpencodeXSessionUiState | undefined,
  time: number,
  sessionUpdated = 0,
): OpencodeXSessionUiState {
  return {
    sessionID,
    seenAt: Math.max(time, current?.seenAt ?? 0),
    ...(current?.reviewedAt === undefined ? {} : { reviewedAt: current.reviewedAt }),
    // Viewing clears an explicit unread mark, matching the server's seen flow.
    // markedUnreadAt is deliberately dropped here.
    revision: current?.revision ?? 0,
    reviewedFiles: current?.reviewedFiles ?? [],
    displayStatus: current?.displayStatus ?? "idle",
    updated: sessionUpdated > time,
  }
}
