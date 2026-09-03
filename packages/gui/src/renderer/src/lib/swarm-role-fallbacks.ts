import type { OpencodeXSwarmRoleInput } from "@opencode-ai/sdk/v2/client"

export type SwarmRoleFallbackModel = NonNullable<OpencodeXSwarmRoleInput["fallbackModels"]>[number]

export const MAX_SWARM_ROLE_FALLBACKS = 4

export function canAddSwarmRoleFallback(fallbacks: readonly SwarmRoleFallbackModel[]) {
  return fallbacks.length < MAX_SWARM_ROLE_FALLBACKS
}

export function swarmRoleModelKey(model: Pick<SwarmRoleFallbackModel, "providerID" | "modelID">) {
  return `${model.providerID}\0${model.modelID}`
}

export function canSelectSwarmRoleModel(
  role: OpencodeXSwarmRoleInput,
  model: SwarmRoleFallbackModel,
  target: "primary" | number | "new",
) {
  const used = [
    ...(target === "primary" || !role.providerID || !role.modelID
      ? []
      : [{ providerID: role.providerID, modelID: role.modelID }]),
    ...(role.fallbackModels ?? []).filter((_, index) => index !== target),
  ]
  return !used.some((item) => swarmRoleModelKey(item) === swarmRoleModelKey(model))
}

export function setSwarmRoleFallback(
  fallbacks: readonly SwarmRoleFallbackModel[],
  index: number | "new",
  model: SwarmRoleFallbackModel,
) {
  if (index === "new") return [...fallbacks, model]
  return fallbacks.map((current, currentIndex) => (currentIndex === index ? model : current))
}

export function removeSwarmRoleFallback(fallbacks: readonly SwarmRoleFallbackModel[], index: number) {
  return fallbacks.filter((_, currentIndex) => currentIndex !== index)
}

export function moveSwarmRoleFallback(
  fallbacks: readonly SwarmRoleFallbackModel[],
  index: number,
  direction: -1 | 1,
) {
  const target = index + direction
  if (target < 0 || target >= fallbacks.length) return [...fallbacks]
  return fallbacks.map((model, currentIndex) => {
    if (currentIndex === index) return fallbacks[target]
    if (currentIndex === target) return fallbacks[index]
    return model
  })
}
