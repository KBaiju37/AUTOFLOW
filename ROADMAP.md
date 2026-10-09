# AutoFlow completion roadmap

This roadmap describes work still needed before AutoFlow should be described as production-ready. A passing unit suite is necessary but not sufficient.

## Completed in this iteration

- Added an optional FastAPI service (`autoflow[api]`) for record-based profile/validate requests, connector inventory, and health checks. The service has configurable payload/row/column limits and deliberately does not accept arbitrary filesystem paths or connection strings.
- Added HTTP-service tests and CI for Python 3.11–3.13 with optional dependencies.
- Verified a wheel can be built in the local build environment.
- Local-file output now writes quarantine-only batches when the gate blocks approved rows.
- Dataset names are sanitized before being used in output filenames.
- Local-file batches publish a JSON manifest last as the commit marker. Interrupted writes can leave uncommitted files; consumers must ignore batches without a manifest and retries can replace orphaned files.
- Regression tests cover blocked quarantine output and path sanitization.

## Next engineering milestones

1. **Persistence and failure semantics**
   - Add fault-injection tests for interruption between approved/quarantine writes and manifest publication.
   - Verify idempotency for gate failures, partial success, and retries after incomplete local-file writes.
   - Document separate SQLite store boundaries and local-file manifest consumption.
2. **Security and configuration hardening**
   - Review URL, SQL query, environment-variable, exception, and audit redaction paths.
   - Test path traversal, malicious configuration values, and malformed input sizes.
   - Define retention and access controls for audit/quarantine data.
3. **Connector verification**
   - Add optional-dependency CI jobs for Excel, Parquet/Arrow, SQL dialect parsing and database integration.
   - Add a local mock-server test for REST pagination, timeouts, retries, and HTTP errors.
4. **Scale and reliability**
   - Benchmark memory and throughput on large CSV/JSONL sources.
   - Specify whether whole-dataset validation can remain in-memory or needs a streaming validation engine.
   - Add repeatable performance fixtures and concurrency tests.
5. **Operator experience**
   - Add machine-readable JSON schemas and stable CLI exit-code tests.
   - Improve actionable diagnostics, metrics, and run reports.
   - Consider HTTP API, dashboard, and orchestration adapters only after the core contracts stabilize.
6. **Release quality**
   - CI now covers Python 3.11–3.13 with optional dependencies; broaden pandas/version coverage, add dependency/security scanning, and finalize a release checklist.
   - Do not label the platform production-ready until each required connector and operational guarantee has evidence.
