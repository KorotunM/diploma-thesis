import type { FrontendRuntime } from "../../shared/runtime";

import type { UniversityCardSnapshot } from "./models";

export async function loadUniversityCard(options: {
  runtime: FrontendRuntime;
  universityId: string;
  signal?: AbortSignal;
}): Promise<UniversityCardSnapshot> {
  const card = await options.runtime.backendApi.getUniversityCard(
    options.universityId,
    { signal: options.signal },
  );

  return {
    universityId: options.universityId,
    card,
    receivedAt: new Date().toISOString(),
  };
}
