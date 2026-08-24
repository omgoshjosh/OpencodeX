import type { OpencodeXSwarmFallbackModel, OpencodeXSwarmRoleInput, Provider } from "@opencode-ai/sdk/v2/client"

export const MAX_SWARM_ROLE_FALLBACKS = 4

export function canAddSwarmRoleFallback(fallbacks: readonly OpencodeXSwarmFallbackModel[]) {
  return fallbacks.length < MAX_SWARM_ROLE_FALLBACKS
}

export function swarmRoleModelKey(model: Pick<OpencodeXSwarmFallbackModel, "providerID" | "modelID">) {
  return `${model.providerID}\0${model.modelID}`
}

export function canSelectSwarmRoleModel(
  role: OpencodeXSwarmRoleInput,
  model: OpencodeXSwarmFallbackModel,
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

export function swarmRoleFallbackCatalogIssue(
  fallback: OpencodeXSwarmFallbackModel,
  providers: readonly Provider[],
  connectedProviderIDs: readonly string[],
) {
  const provider = providers.find((item) => item.id === fallback.providerID)
  if (!provider) return "Provider is unavailable."
  const model = provider.models[fallback.modelID]
  if (!model) return "Model is unavailable."
  if (fallback.variant && fallback.variant !== "default" && !model.variants?.[fallback.variant]) {
    return `Variant ${fallback.variant} is unavailable.`
  }
  if (!connectedProviderIDs.includes(fallback.providerID)) return `${provider.name} is not connected.`
}

export function setSwarmRoleFallback(
  fallbacks: readonly OpencodeXSwarmFallbackModel[],
  index: number | "new",
  model: OpencodeXSwarmFallbackModel,
) {
  if (index === "new") return [...fallbacks, model]
  return fallbacks.map((current, currentIndex) => (currentIndex === index ? model : current))
}

export function removeSwarmRoleFallback(fallbacks: readonly OpencodeXSwarmFallbackModel[], index: number) {
  return fallbacks.filter((_, currentIndex) => currentIndex !== index)
}

export function moveSwarmRoleFallback(
  fallbacks: readonly OpencodeXSwarmFallbackModel[],
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
