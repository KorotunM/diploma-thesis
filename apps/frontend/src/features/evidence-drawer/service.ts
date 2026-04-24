import type { ClaimTraceDto, UniversityProvenanceDto } from "../../shared/backend-api";
import type { FrontendRuntime } from "../../shared/runtime";

import type {
  EvidenceChainEntry,
  EvidenceDrawerSnapshot,
  FieldAttributionSummary,
} from "./models";

export async function loadEvidenceDrawer(options: {
  runtime: FrontendRuntime;
  universityId: string;
  signal?: AbortSignal;
}): Promise<EvidenceDrawerSnapshot> {
  const provenance = await options.runtime.backendApi.getUniversityProvenance(
    options.universityId,
    { signal: options.signal },
  );

  return {
    universityId: options.universityId,
    provenance,
    fieldAttributions: buildFieldAttributions(provenance),
    evidenceChain: buildEvidenceChain(provenance),
    receivedAt: new Date().toISOString(),
  };
}

function buildFieldAttributions(
  provenance: UniversityProvenanceDto,
): FieldAttributionSummary[] {
  const canonicalNameSources = provenance.delivery_projection.card.canonical_name.sources.map(
    (source) => ({
      fieldName: "canonical_name",
      confidence: provenance.delivery_projection.card.canonical_name.confidence,
      sourceKey: source.source_key,
      sourceUrls: [source.source_url],
      evidenceIds: source.evidence_ids,
    }),
  );

  const resolvedFacts = provenance.resolved_facts.map((fact) => ({
    fieldName: fact.field_name,
    confidence: fact.fact_score,
    sourceKey:
      typeof fact.metadata.source_key === "string" ? fact.metadata.source_key : null,
    sourceUrls: Array.isArray(fact.metadata.source_urls)
      ? fact.metadata.source_urls.filter((value): value is string => typeof value === "string")
      : [],
    evidenceIds: fact.selected_evidence_ids,
  }));

  const byField = new Map<string, FieldAttributionSummary>();
  [...resolvedFacts, ...canonicalNameSources].forEach((entry) => {
    const existing = byField.get(entry.fieldName);
    if (existing === undefined) {
      byField.set(entry.fieldName, {
        fieldName: entry.fieldName,
        confidence: entry.confidence,
        sourceKey: entry.sourceKey,
        sourceUrls: dedupeStrings(entry.sourceUrls),
        evidenceIds: dedupeStrings(entry.evidenceIds),
      });
      return;
    }
    byField.set(entry.fieldName, {
      fieldName: entry.fieldName,
      confidence: Math.max(existing.confidence, entry.confidence),
      sourceKey: existing.sourceKey ?? entry.sourceKey,
      sourceUrls: dedupeStrings([...existing.sourceUrls, ...entry.sourceUrls]),
      evidenceIds: dedupeStrings([...existing.evidenceIds, ...entry.evidenceIds]),
    });
  });

  return [...byField.values()].sort((left, right) =>
    left.fieldName.localeCompare(right.fieldName),
  );
}

function buildEvidenceChain(
  provenance: UniversityProvenanceDto,
): EvidenceChainEntry[] {
  const claimsById = new Map(provenance.claims.map((claim) => [claim.claim_id, claim]));
  const parsedDocumentById = new Map(
    provenance.parsed_documents.map((document) => [document.parsed_document_id, document]),
  );
  const rawArtifactById = new Map(
    provenance.raw_artifacts.map((artifact) => [artifact.raw_artifact_id, artifact]),
  );
  const factFieldsByEvidenceId = new Map<string, Set<string>>();

  provenance.resolved_facts.forEach((fact) => {
    fact.selected_evidence_ids.forEach((evidenceId) => {
      const fields = factFieldsByEvidenceId.get(evidenceId) ?? new Set<string>();
      fields.add(fact.field_name);
      factFieldsByEvidenceId.set(evidenceId, fields);
    });
  });

  return provenance.claim_evidence
    .map((evidence) => {
      const claim = claimsById.get(evidence.claim_id);
      const parsedDocument = claim
        ? parsedDocumentById.get(claim.parsed_document_id)
        : undefined;
      const rawArtifact = rawArtifactById.get(evidence.raw_artifact_id);

      return {
        evidenceId: evidence.evidence_id,
        sourceKey: evidence.source_key,
        sourceUrl: evidence.source_url,
        capturedAt: evidence.captured_at,
        fieldNames: dedupeStrings([
          ...(factFieldsByEvidenceId.get(evidence.evidence_id) ?? []),
          claim?.field_name ?? "",
        ]),
        parserVersions: dedupeStrings(collectParserVersions(claim, parsedDocument)),
        httpStatus: rawArtifact?.http_status ?? null,
        storageObjectKey: rawArtifact?.storage_object_key ?? null,
      };
    })
    .sort((left, right) => right.capturedAt.localeCompare(left.capturedAt));
}

function collectParserVersions(
  claim: ClaimTraceDto | undefined,
  parsedDocument:
    | UniversityProvenanceDto["parsed_documents"][number]
    | undefined,
): string[] {
  return dedupeStrings([
    claim?.parser_version ?? "",
    parsedDocument?.parser_version ?? "",
  ]);
}

function dedupeStrings(values: Iterable<string>): string[] {
  const result = new Set<string>();
  for (const value of values) {
    if (value) {
      result.add(value);
    }
  }
  return [...result];
}
