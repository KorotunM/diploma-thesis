export interface HealthResponseDto {
  service: string;
  status: "ok";
  environment: string;
  version: string;
  dependencies: Record<string, string>;
}
