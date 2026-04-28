export function formatSearchFilters(options: {
  city: string | null | undefined;
  country: string | null | undefined;
  sourceType: string | null | undefined;
}): string {
  const parts = [
    options.city?.trim() ? `city=${options.city.trim()}` : null,
    options.country?.trim() ? `country=${options.country.trim()}` : null,
    options.sourceType?.trim() ? `source=${options.sourceType.trim()}` : null,
  ].filter((value): value is string => value !== null);

  if (parts.length === 0) {
    return "none";
  }
  return parts.join(" | ");
}
