import type { UniversityProvenanceDto } from "../../shared/backend-api";

export interface FieldAttributionSummary {
  fieldName: string;
  confidence: number;
  sourceKey: string | null;
  sourceUrls: string[];
  evidenceIds: string[];
}

export interface EvidenceChainEntry {
  evidenceId: string;
  sourceKey: string;
  sourceUrl: string;
  capturedAt: string;
  fieldNames: string[];
  parserVersions: string[];
  httpStatus: number | null;
  storageObjectKey: string | null;
}

export interface EvidenceDrawerSnapshot {
  universityId: string;
  provenance: UniversityProvenanceDto;
  fieldAttributions: FieldAttributionSummary[];
  evidenceChain: EvidenceChainEntry[];
  receivedAt: string;
}
