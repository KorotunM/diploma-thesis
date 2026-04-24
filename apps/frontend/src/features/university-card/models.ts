import type { UniversityCardDto } from "../../shared/backend-api";

export interface UniversityCardSnapshot {
  universityId: string;
  card: UniversityCardDto;
  receivedAt: string;
}
