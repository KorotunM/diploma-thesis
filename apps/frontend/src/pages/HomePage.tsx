import { useFrontendRuntime } from "../shared/runtime";

export function HomePage() {
  const { config } = useFrontendRuntime();

  return (
    <section className="panel">
      <h2>HomePage</h2>
      <p>
        Entry point for overview metrics, source freshness, indexing status and quick navigation.
      </p>
      <div className="stat-row">
        <span>environment</span>
        <strong>{config.appEnvironment}</strong>
      </div>
      <div className="stat-row">
        <span>backend</span>
        <strong>{config.backendBaseUrl}</strong>
      </div>
    </section>
  );
}
