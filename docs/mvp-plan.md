# Lighthouse MVP Plan — "Follow the Money (Câmara edition)"

Derived from [PRD.md](../PRD.md) (§24–25 V0 scope) and [lighthouse_sources.yaml](../lighthouse_sources.yaml).

## 1. MVP goal

For any federal deputy, Lighthouse answers the five V0 questions, and every answer is traceable to the original source record:

| # | Question | Answered by |
|---|---|---|
| Q1 | How much did politician X spend? | `gold.politician_expense_monthly` |
| Q2 | Who received that money? | `gold.supplier_political_payments` |
| Q3 | Is the spending unusual relative to peers? | `gold.expense_peer_benchmark` + signals |
| Q4 | How have the politician's declared assets changed? | `gold.politician_asset_history` + signals |
| Q5 | What evidence supports those answers? | Evidence resolver + finding envelope |

**Definition of done:** `make up && make run` on a clean machine ingests the MVP sources, builds Bronze → Silver → Gold Iceberg tables, generates signals, and serves an API whose responses each carry a provenance block (source URL, source record id, landing file checksum, ingestion run id).

## 2. Scope

| Dimension | In MVP | Deferred |
|---|---|---|
| Sources | Câmara: `deputados`, `deputado_detalhes`, `despesas` (CEAP bulk CSV). TSE: `candidatos`, `bens_candidatos` | Senado, Portal da Transparência, PNCP, Receita, IBGE, Tesouro, BCB, Base dos Dados |
| Time window | CEAP 2019–2026 (legislatures 56–57); TSE elections 2018, 2022, 2026 | CEAP backfill to 2008 |
| Entities | politician, expense, supplier, election (candidacy), asset; party as attribute | company, vote, proposition, municipality, donation |
| Signals | `expense_outlier`, `supplier_concentration`, `asset_growth`, `duplicate_expense` | `cross_source_value_mismatch`, shared-supplier graph, ML |
| Interface | FastAPI read API + generated "Follow the Money" report | Explorer UI, agent, MCP server |
| Semantic layer | Minimal YAML metric definitions | MetricFlow / full semantic layer |
| Infra | Docker Compose, single machine | Streaming (V1), Terraform, cloud |

## 3. Architecture

```text
 Câmara API / CEAP zip        TSE CDN zip (CKAN)
          │                          │
          ▼                          ▼
   Python ingestors (Dagster assets, httpx + retries)
          │
          ├──► landing/  (MinIO, original files, immutable, checksummed)
          │
          ▼
   PyIceberg append ──► bronze.*  (Iceberg, raw columns + provenance columns)
                              │
                     dbt-trino (Dagster-orchestrated)
                              ▼
                        silver.*  (typed, normalized, entity-resolved)
                              ▼
                        gold.*    (entities, aggregates, benchmarks)
                              ▼
                        signals.* (rule-based, versioned)
                              │
                 Trino ◄──────┴──────► FastAPI ──► API / report
```

| Layer | Technology |
|---|---|
| Object storage | MinIO |
| Table format | Apache Iceberg (Bronze, Silver, Gold, Signals) |
| Catalog | Iceberg REST catalog (Postgres-backed) |
| Bronze writes | PyIceberg (Arrow) |
| Transformations | dbt-trino |
| Query engine | Trino (Iceberg connector) |
| Orchestration | Dagster (+ `dagster-dbt`) |
| Quality | dbt tests |
| API | FastAPI (queries Trino) |
| Packaging | uv, Docker Compose |

**Design rules**
- Original files are kept unchanged in `landing/`, and Bronze rows reference them by `_landing_path` and `_checksum`.
- Every Silver/Gold row carries `bronze_record_id` (or an array of them for aggregates) back to Bronze.
- Signals describe observations against a baseline. They never use the words fraud or wrongdoing.
- The API exposes predefined endpoints only. It never executes arbitrary SQL.

## 4. Epic overview & dependencies

```text
E0 Foundation ─► E1 Ingestion framework ─┬─► E2 Câmara ─┐
                                         └─► E3 TSE ────┤
                                                        ▼
                                          E4 Silver & entity resolution
                                                        ▼
                                          E5 Gold & metrics
                                                 ┌──────┴──────┐
                                                 ▼             ▼
                                          E6 Signals     E7 Evidence
                                                 └──────┬──────┘
                                                        ▼
                                                   E8 Serving
E9 Quality & docs ── runs in parallel throughout
```

| Epic | Title | Outcome |
|---|---|---|
| E0 | Foundation & Lakehouse Platform | Local Iceberg lakehouse running via Compose |
| E1 | Ingestion Framework & Provenance | Reusable, idempotent, provenance-capturing ingestion |
| E2 | Câmara Ingestion | Deputies + CEAP expenses in Bronze |
| E3 | TSE Ingestion | Candidates + declared assets in Bronze |
| E4 | Silver: Standardization & Entity Resolution | Canonical politicians and suppliers |
| E5 | Gold: Analytical Models & Metrics | Tables answering Q1–Q4 |
| E6 | Signals Engine | Versioned rule-based signals |
| E7 | Evidence & Provenance | Q5: every number traceable |
| E8 | Serving | API + Follow the Money report |
| E9 | Quality, Observability & Docs | Tests, freshness, documentation |

---

## E0 — Foundation & Lakehouse Platform

### S0.1 Repository scaffold
Set up the repository layout from PRD §23, trimmed to the MVP.

**Acceptance criteria**
- Folders exist: `ingestion/`, `lakehouse/`, `transformations/dbt/`, `semantic/`, `signals/`, `api/`, `orchestration/`, `infrastructure/docker/`, `tests/`, `docs/`.
- `uv sync` installs the workspace; `ruff` and pre-commit hooks pass on a clean checkout.
- The README contains the one-sentence thesis (PRD §30) and quickstart.

**Sub-tasks**
- [ ] Create folder structure and `pyproject.toml` (uv workspace)
- [ ] Configure ruff, mypy (lenient), pre-commit
- [ ] Add `.gitignore`, `.env.example`, `LICENSE`
- [ ] Write README skeleton

### S0.2 Local lakehouse stack
Run MinIO, the Iceberg REST catalog and Trino via Docker Compose.

**Acceptance criteria**
- `make up` starts MinIO, Postgres (catalog backend), Iceberg REST catalog and Trino. Health checks pass.
- Buckets `landing` and `warehouse` are created automatically.
- From Trino: `CREATE SCHEMA iceberg.bronze`, then create, insert into and select from a test table. The data files appear in MinIO.
- PyIceberg can load the same catalog and read the test table.

**Sub-tasks**
- [ ] Compose services: minio, minio-init (mc), postgres, iceberg-rest, trino
- [ ] Trino `iceberg.properties` (REST catalog, native S3 to MinIO, path-style)
- [ ] Bootstrap script creating schemas `bronze`, `silver`, `gold`, `signals`, `ops`
- [ ] PyIceberg config (`.pyiceberg.yaml` / env vars)
- [ ] Smoke test script `make smoke`

### S0.3 Orchestration stack
Run Dagster (webserver + daemon) in Compose.

**Acceptance criteria**
- The Dagster UI is reachable and loads a code location from `orchestration/`.
- A hello-world asset that writes to Iceberg materializes successfully.

**Sub-tasks**
- [ ] Dagster Dockerfile and `workspace.yaml`
- [ ] Shared resources: `TrinoResource`, `IcebergCatalogResource`, `S3Resource`
- [ ] Makefile targets: `up`, `down`, `run`, `transform`, `test`

### S0.4 CI pipeline
**Acceptance criteria**
- GitHub Actions runs lint, unit tests, and `dbt build` against the Compose stack using fixture data.

**Sub-tasks**
- [ ] Workflow: lint + unit tests
- [ ] Workflow job: start Compose, seed fixtures, `dbt build`
- [ ] Cache uv and Docker layers

---

## E1 — Ingestion Framework & Provenance

### S1.1 Source registry loader
Load `lighthouse_sources.yaml` into typed config used by ingestors.

**Acceptance criteria**
- Pydantic models cover `project`, `defaults`, `sources`, `datasets`, `canonical_entities`, `signals`.
- Invalid YAML (unknown tier, missing id, duplicate dataset id) fails validation in CI.
- Ingestors read URLs, schedules and load strategies from the registry, not hard-coded values.

**Sub-tasks**
- [ ] Pydantic models + defaults merging
- [ ] `lighthouse registry validate` CLI command
- [ ] Unit tests with valid/invalid fixtures

### S1.2 Landing writer
Persist original source payloads unchanged.

**Acceptance criteria**
- Files are written to `landing/{source}/{dataset}/{partition}/{run_id}/{filename}`.
- Writes compute a SHA-256 checksum. A file is never overwritten.
- A sidecar `_manifest.json` stores `source_url`, `retrieved_at`, `source_updated_at` (from HTTP headers when available), `checksum`, `schema_version`, `ingestion_run_id`.

**Sub-tasks**
- [ ] `LandingWriter` class (S3 put-if-absent semantics)
- [ ] Checksum + manifest generation
- [ ] Unit tests with moto/MinIO

### S1.3 Bronze Iceberg loader
Load landed files into Bronze Iceberg tables with provenance columns.

**Acceptance criteria**
- Source columns are kept as strings (no type coercion in Bronze).
- Provenance columns are added: `_source_id`, `_dataset_id`, `_source_url`, `_landing_path`, `_checksum`, `_retrieved_at`, `_source_updated_at`, `_schema_version`, `_ingestion_run_id`, `_row_number`, and `_bronze_record_id` (deterministic hash of checksum + row number).
- Bronze tables are partitioned by dataset partition (e.g. `year`).
- When the source schema changes, the table schema evolves additively (new columns) and `_schema_version` increments.

**Sub-tasks**
- [ ] CSV/JSON → Arrow readers (encoding + delimiter configurable)
- [ ] PyIceberg table creation with partition spec
- [ ] Schema-evolution handling (`union_by_name`)
- [ ] Tests: provenance columns present and deterministic

### S1.4 Ingestion run ledger & idempotency
**Acceptance criteria**
- The `ops.ingestion_runs` Iceberg table records run id, source, dataset, partition, status, row count, checksum, started/finished timestamps.
- If a file's checksum already loaded successfully for the same dataset/partition, the Bronze load is skipped and the run is recorded as `skipped_duplicate`.
- Re-running a full ingestion produces no duplicate Bronze rows.

**Sub-tasks**
- [ ] Ledger table + writer
- [ ] Checksum lookup before load
- [ ] Integration test: run twice, assert row count unchanged

### S1.5 Resilient HTTP client
**Acceptance criteria**
- Retries with exponential backoff up to `defaults.retry.max_attempts` (5).
- Configurable rate limit per source. Handles Câmara page pagination (`itens=100`, `links.next`).
- Streams large downloads to disk/MinIO without loading them into memory.

**Sub-tasks**
- [ ] httpx client wrapper with tenacity
- [ ] Paginator for Câmara REST
- [ ] Streaming download helper
- [ ] Tests with mocked transport

---

## E2 — Câmara Ingestion

### S2.1 Deputies snapshot & details
**Acceptance criteria**
- `/deputados?idLegislatura=56` and `57` are landed and loaded into `bronze.camara_deputados`.
- `/deputados/{id}` is fetched for every deputy and loaded into `bronze.camara_deputado_detalhes` (civil name, CPF, birth date, birth state, ...).
- Runs are idempotent (S1.4).

**Sub-tasks**
- [ ] Deputies list ingestor (per legislature)
- [ ] Details fan-out ingestor with rate limiting
- [ ] Dagster assets `camara_deputados`, `camara_deputado_detalhes`

### S2.2 CEAP expenses — bulk backfill
**Acceptance criteria**
- `Ano-{year}.csv.zip` for 2019–2026 is landed as the original zip, then extracted and loaded into `bronze.camara_despesas`, partitioned by `year`.
- Bronze row count equals the CSV row count for every year (asserted in the ledger).
- `ideDocumento` and `urlDocumento` are preserved for evidence.

**Sub-tasks**
- [ ] Zip download + extraction (streamed)
- [ ] CSV reader config (`;` delimiter, encoding detection, BOM handling)
- [ ] Partitioned Dagster asset over years
- [ ] Row-count assertion

### S2.3 CEAP incremental refresh
Re-pull the current year's file on a schedule and only load it when it has changed.

**Acceptance criteria**
- A scheduled job (registry `suggested_schedule`) downloads the current-year file. When its checksum differs, a new Bronze snapshot partition (`run_id`) is loaded.
- Silver deduplicates by `ideDocumento`, keeping the latest snapshot (handled in S4.1).
- The ledger shows changed vs unchanged runs. This is the intended hook for V1 streaming.

**Sub-tasks**
- [ ] Current-year partition job
- [ ] Dagster schedule from registry cron
- [ ] Change-detection test

---

## E3 — TSE Ingestion

### S3.1 TSE dataset discovery
**Acceptance criteria**
- Download URLs for `consulta_cand` and `bem_candidato` for 2018, 2022 and 2026 are resolved via the CKAN API (with CDN URL fallback) and recorded in the run manifest.
- A missing year (e.g. a 2026 file not yet published) produces a warning, not a failure.

**Sub-tasks**
- [ ] CKAN `package_show` client
- [ ] Resource selection by year/name
- [ ] Fallback to known CDN URL patterns

### S3.2 Candidates & declared assets load
**Acceptance criteria**
- Original zips are landed. The national (`BRASIL`) file is preferred and per-UF files are used otherwise.
- `bronze.tse_candidatos` and `bronze.tse_bens_candidatos` are loaded, partitioned by `election_year`.
- Latin-1 encoding and `;` delimiter are handled. The `#NULO#` / `#NE#` sentinel values are preserved in Bronze unchanged.

**Sub-tasks**
- [ ] Zip handling (multiple CSVs per zip)
- [ ] Reader config for TSE files
- [ ] Partitioned Dagster assets by election year
- [ ] Row-count assertions

---

## E4 — Silver: Standardization & Entity Resolution

### S4.0 Spike — identity keys across Câmara and TSE (time-boxed: 2 days)
Find out which keys can reliably link the same person across sources and across election years.

**Acceptance criteria**
- A written finding in `docs/spikes/identity-keys.md` covers, per source/year: availability and masking of the CPF (Câmara details, CEAP `cpf`, TSE `NR_CPF_CANDIDATO`), the TSE voter ID (`NR_TITULO_ELEITORAL_CANDIDATO`), birth date and name fields.
- Measured match rate between Câmara deputies (legislature 57) and TSE 2022 elected federal deputies, using (a) CPF and (b) normalized name + birth date + UF.
- A recommended matching strategy for S4.3, and a recommended key for linking TSE candidacies across 2018/2022/2026 for S5.5.

**Sub-tasks**
- [ ] Profile identity columns in Bronze (null / masked rates)
- [ ] Prototype both match strategies in SQL on Trino
- [ ] Write findings + recommendation

### S4.1 Staging models
**Acceptance criteria**
- `silver.stg_camara_deputados`, `stg_camara_despesas`, `stg_tse_candidatos`, `stg_tse_bens` exist as dbt models (Iceberg tables via dbt-trino).
- Types are cast (decimals, dates); `#NULO#` and empty strings become NULL; column names are snake_case English.
- CEAP is deduplicated by `ideDocumento`, keeping the latest snapshot. Value used: `vlrLiquido`, with `vlrDocumento` and `vlrGlosa` retained.
- Every row keeps `bronze_record_id`.

**Sub-tasks**
- [ ] dbt project setup (`dbt-trino`, Iceberg table format, schemas)
- [ ] Source definitions over `bronze.*`
- [ ] Staging models + column docs
- [ ] Tests: not-null keys, unique `ideDocumento`

### S4.2 Name & document normalization macros
**Acceptance criteria**
- dbt macros/UDF-equivalents: `normalize_name` (strip accents, uppercase, collapse whitespace, remove honorifics such as "DR.", "DEP."), `clean_document` (digits only), `is_valid_cnpj`, `is_valid_cpf`, `hash_cpf` (salted SHA-256, salt from env).
- Unit tests cover known edge cases (accents, "JOAO P. DA SILVA" vs "JOÃO PEDRO DA SILVA").

**Sub-tasks**
- [ ] Macros in Trino SQL
- [ ] Check-digit validation logic
- [ ] dbt unit tests

### S4.3 Politician entity resolution (Câmara ↔ TSE)
**Acceptance criteria**
- `silver.dim_politician` holds `politician_id` (`br:politician:{uuid}`, stable across runs), `canonical_name`, `birth_date`, `cpf_hash`, `camara_id`, `tse_person_key`, `state`, `party`.
- `silver.entity_alias` holds `source`, `source_entity_id`, `politician_id`, `source_name`, `confidence`, `resolution_method`.
- Matching follows the S4.0 recommendation: deterministic key first, then a name + birth date + UF fallback with a confidence score.
- IDs stay stable: re-running does not reassign existing `politician_id`s (persisted ID map).
- At least 98% of legislature-57 deputies are linked to a TSE candidacy (target to be confirmed by S4.0).

**Sub-tasks**
- [ ] Deterministic match model
- [ ] Fallback match model with confidence scoring
- [ ] Persisted ID map (incremental model) for stable UUIDs
- [ ] Tests: one politician_id per camara_id; confidence in [0,1]

### S4.4 Supplier resolution
**Acceptance criteria**
- `silver.dim_supplier` holds `supplier_id` (`br:supplier:{cnpj}` or `br:supplier:cpf:{hash}` or `br:supplier:name:{hash}` when no valid document), `document_type`, `canonical_name`, `is_document_valid`.
- Supplier name variants are recorded in `entity_alias`.
- No raw CPF of an individual supplier appears in Silver/Gold.

**Sub-tasks**
- [ ] Document cleaning + classification (CNPJ/CPF/none)
- [ ] Canonical name selection (most frequent variant)
- [ ] Tests: unique supplier per valid CNPJ; no 11-digit raw CPFs

### S4.5 Resolution review output
**Acceptance criteria**
- `silver.resolution_review` lists unmatched deputies and matches below a confidence threshold, with candidate pairs side by side.
- A count of review items is shown in the Dagster asset metadata.

**Sub-tasks**
- [ ] Review model
- [ ] Dagster asset check surfacing counts

### S4.6 Canonical ID specification
**Acceptance criteria**
- `docs/data-model.md` documents ID formats for politician, supplier, expense (`br:expense:camara:{ideDocumento}`), election (`br:election:{year}:federal`) and asset, consistent with the registry's `canonical_entities`.

**Sub-tasks**
- [ ] Write spec
- [ ] dbt macro `canonical_id(entity, ...)` used by all models

---

## E5 — Gold: Analytical Models & Metrics

### S5.1 Gold entity tables
**Acceptance criteria**
- `gold.politician`, `gold.expense`, `gold.supplier`, `gold.election_candidacy`, `gold.asset` exist with canonical IDs and documented columns.
- `gold.expense` includes `expense_id`, `politician_id`, `supplier_id`, `category` (from `txtDescricao`), `issue_date`, `year`, `month`, `net_value`, `document_url`, `bronze_record_id`.
- Relationship tests pass (every expense → valid politician and supplier).

**Sub-tasks**
- [ ] Entity models
- [ ] Category normalization seed (CEAP subquota → category label)
- [ ] Relationship + not-null tests

### S5.2 Politician expense aggregates (Q1)
**Acceptance criteria**
- `gold.politician_expense_monthly` has a grain of politician × year × month × category, with `total_net_value`, `expense_count`, and `bronze_record_ids` (array) or a join key to `gold.expense`.
- The total for a sample deputy/year matches a manual sum over the Bronze CSV (fixture test).

**Sub-tasks**
- [ ] Aggregate model
- [ ] Reconciliation test against Bronze

### S5.3 Supplier payments (Q2)
**Acceptance criteria**
- `gold.supplier_political_payments` has a grain of supplier × politician × year, with total, count, and the share of the politician's yearly spend.
- Supplier-level rollup: total received, number of distinct politicians.

**Sub-tasks**
- [ ] Payment fact model
- [ ] Supplier rollup model
- [ ] Tests: shares sum to 1 per politician-year

### S5.4 Peer benchmark (Q3)
**Acceptance criteria**
- `gold.expense_peer_benchmark` has a grain of category × year × month × peer_group (`national`, `state:{UF}`), with p50, p90, p95, mean and n.
- `gold.politician_expense_percentile` holds each politician's percentile per category/period within each peer group.
- Peer groups with n below a minimum (configurable, e.g. 5) are flagged and excluded from signals.

**Sub-tasks**
- [ ] Benchmark model (`approx_percentile` in Trino)
- [ ] Percentile model
- [ ] Minimum-n configuration

### S5.5 Asset history (Q4)
**Acceptance criteria**
- `gold.politician_asset_history` has a grain of politician × election year, with total declared assets, asset count, totals by asset type, and the change from the previous election in both absolute and % terms.
- Links candidacies across years using the S4.0-recommended key.

**Sub-tasks**
- [ ] Asset totals per candidacy
- [ ] Cross-year linking + window functions for growth
- [ ] Tests: growth NULL when no prior election

### S5.6 Minimal semantic layer
**Acceptance criteria**
- `semantic/` YAML defines entities (politician, supplier) and metrics: `total_expenses`, `avg_monthly_expense`, `expense_percentile`, `supplier_concentration`, `declared_assets`, `asset_growth` — each mapped to a Gold table/column with a description.
- A loader validates that every referenced table and column exists (via Trino `information_schema`).
- The API (E8) resolves metric names through this layer, not hard-coded SQL fragments.

**Sub-tasks**
- [ ] YAML schema + Pydantic models
- [ ] Metric definitions
- [ ] Validation command + CI check

---

## E6 — Signals Engine (rule-based)

### S6.1 Signal schema & framework
**Acceptance criteria**
- `signals.signal` Iceberg table: `signal_id`, `signal_type`, `entity_type`, `entity_id`, `period`, `severity` (low/medium/high), `observed_value`, `baseline_value`, `baseline_description`, `evidence_refs` (array of `bronze_record_id` / `expense_id`), `rule_id`, `rule_version`, `generated_at`.
- Signals are deterministic: the same inputs and rule version produce the same `signal_id`.
- Signal wording uses the PRD §9 vocabulary (observation / anomaly / signal). User-facing text never says fraud or wrongdoing.

**Sub-tasks**
- [ ] Table DDL (dbt model)
- [ ] Rule config loader (`signals/rules/*.yaml`)
- [ ] Deterministic ID macro

### S6.2 `expense_outlier`
**Acceptance criteria**
- Flags a politician-category-month whose value is above the peer p95 (national and same-state), with severity by percentile band (e.g. ≥95 low, ≥98 medium, ≥99.5 high — set in config).
- Evidence refs list the contributing expense records.

**Sub-tasks**
- [ ] dbt model `signals.sig_expense_outlier`
- [ ] Fixture test with a planted outlier

### S6.3 `supplier_concentration`
**Acceptance criteria**
- Flags a politician-year where one supplier's share exceeds a configured threshold (e.g. 20%), excluding categories where a single supplier is structurally expected (configurable allow-list, e.g. airline tickets).

**Sub-tasks**
- [ ] Model + allow-list seed
- [ ] Fixture test

### S6.4 `asset_growth`
**Acceptance criteria**
- Flags a change in declared assets between consecutive elections above a configured % and absolute floor (e.g. +100% and > R$ 200k), with baseline = previous declaration.

**Sub-tasks**
- [ ] Model
- [ ] Fixture test incl. zero-baseline edge case

### S6.5 `duplicate_expense`
**Acceptance criteria**
- Flags expense pairs with the same politician, supplier, document number, net value and issue date but different `ideDocumento`.

**Sub-tasks**
- [ ] Model
- [ ] Fixture test

### S6.6 Signal consolidation
**Acceptance criteria**
- `signals.signal` unions all rule outputs. Dagster runs signals after Gold and records counts per type and severity as asset metadata.

**Sub-tasks**
- [ ] Union model
- [ ] Dagster asset checks (counts, no NULL evidence)

---

## E7 — Evidence & Provenance

### S7.1 End-to-end lineage columns
**Acceptance criteria**
- Every Silver/Gold/Signal row can be joined back to `bronze.*` through `bronze_record_id`, either directly or through `gold.expense`.
- A dbt test fails if any Gold expense lacks a resolvable Bronze record.

**Sub-tasks**
- [ ] Audit models for lineage propagation
- [ ] Generic dbt test `has_bronze_lineage`

### S7.2 Evidence resolver
**Acceptance criteria**
- Python service: given a `bronze_record_id`, `expense_id` or `signal_id`, returns the original source fields, `source_url`, document URL (CEAP `urlDocumento` where present), `_landing_path`, `_checksum`, `_retrieved_at` and `_ingestion_run_id`.
- Given a landing path, it can recompute the checksum and confirm the file is intact.

**Sub-tasks**
- [ ] Resolver queries via Trino
- [ ] Checksum verification utility
- [ ] Unit + integration tests

### S7.3 Finding envelope
**Acceptance criteria**
- Every analytical API response is wrapped as `{claim, value, calculation, metric, sources[], generated_at, pipeline_version}`, as in PRD §15.
- `calculation` names the semantic metric and Gold model; `sources[]` lists datasets plus record refs (paginated / truncated with a count).

**Sub-tasks**
- [ ] Pydantic envelope model
- [ ] Pipeline version stamp (git SHA + dbt run id)

---

## E8 — Serving

### S8.1 Read API
**Acceptance criteria**
- FastAPI endpoints over Trino:
  - `GET /politicians?q=` (normalized-name search)
  - `GET /politicians/{id}`
  - `GET /politicians/{id}/expenses?year=&category=` (Q1)
  - `GET /politicians/{id}/suppliers?year=` (Q2)
  - `GET /politicians/{id}/benchmark?year=&category=` (Q3)
  - `GET /politicians/{id}/assets` (Q4)
  - `GET /politicians/{id}/signals`
  - `GET /evidence/{ref}` (Q5)
- Queries are parameterized and there is no arbitrary SQL. p95 latency is under 2s on the local stack for a single deputy.
- Responses use the finding envelope (S7.3).

**Sub-tasks**
- [ ] FastAPI app + Trino client dependency
- [ ] Query layer driven by the semantic YAML
- [ ] Endpoints + response models
- [ ] API tests against fixture data

### S8.2 API documentation
**Acceptance criteria**
- OpenAPI docs include examples for every endpoint, and the responsible-use note appears in the API description.

**Sub-tasks**
- [ ] Response examples
- [ ] Description + disclaimer text

### S8.3 "Follow the Money" report
**Acceptance criteria**
- `lighthouse report --politician <id|name> --year 2025` produces a Markdown/HTML report: totals by category, top suppliers, peer comparison, asset history, signals with context, and an evidence appendix with source links (PRD §21).
- Built only from API responses, so it doubles as an end-to-end demo.

**Sub-tasks**
- [ ] Report template (Jinja)
- [ ] CLI command
- [ ] Sample report committed under `docs/examples/`

---

## E9 — Quality, Observability & Docs

### S9.1 Data quality tests
**Acceptance criteria**
- dbt tests cover uniqueness, not-null, relationships and accepted values across Silver/Gold.
- Row-count reconciliation Bronze → Silver for CEAP (allowing only documented dedup drops).
- `dbt build` fails CI on test failure.

**Sub-tasks**
- [ ] Schema tests per model
- [ ] Reconciliation tests
- [ ] Severity config (warn vs error)

### S9.2 Freshness & run observability
**Acceptance criteria**
- dbt source freshness on Bronze `_retrieved_at`. Dagster asset checks for freshness and row-count anomalies (e.g. a ±50% change vs the previous run).
- The run summary (sources, rows, skipped duplicates, signals by type) is visible in Dagster metadata.

**Sub-tasks**
- [ ] Freshness config
- [ ] Asset checks
- [ ] Run summary metadata

### S9.3 Documentation
**Acceptance criteria**
- `docs/architecture.md`, `docs/data-model.md`, `docs/data-sources.md` (generated from the registry), `docs/signals.md` (rule definitions + thresholds), `docs/responsible-use.md` (signals ≠ proof, privacy handling of CPF).
- `dbt docs` is generated and reachable locally.

**Sub-tasks**
- [ ] Write docs
- [ ] Registry → data-sources.md generator
- [ ] Architecture diagram

### S9.4 Golden-path end-to-end test
**Acceptance criteria**
- A fixture dataset (2–3 deputies, 2 years of CEAP, 2 elections of assets, with a planted outlier, concentration, duplicate and asset jump) runs the full pipeline in CI.
- Asserts all five questions are answerable via the API with expected values and non-empty evidence.

**Sub-tasks**
- [ ] Build fixture data
- [ ] E2E test harness (Compose + Dagster job + API calls)

---

## 5. Suggested delivery sequence

| Milestone | Stories | Demo |
|---|---|---|
| M1 — Lakehouse up | S0.1–S0.4, S1.1–S1.5 | Iceberg table written by PyIceberg, read by Trino |
| M2 — Bronze complete | S2.1–S2.3, S3.1–S3.2 | All MVP sources in Bronze with run ledger |
| M3 — Entities resolved | S4.0–S4.6 | `dim_politician` linking Câmara ↔ TSE with confidence |
| M4 — Q1–Q4 answerable | S5.1–S5.6 | SQL in Trino answers Q1–Q4 for a deputy |
| M5 — Signals + evidence | S6.1–S6.6, S7.1–S7.3 | Signal traced back to the original CEAP record |
| M6 — MVP release | S8.1–S8.3, S9.1–S9.4 | Follow the Money report via API |

## 6. Risks & open items

| Risk | Impact | Mitigation |
|---|---|---|
| CPF masked in TSE recent years | Lower politician match rate | S4.0 spike; voter-ID / name + birth date fallback; review queue (S4.5) |
| TSE 2026 files incomplete or late | Q4 missing latest election | S3.1 tolerates missing years; asset growth uses latest available pair |
| dbt-trino incremental/merge edge cases on Iceberg | Rebuild cost, duplicate rows | Prefer `table` materialization in MVP; incremental only for the ID map |
| Peer benchmarks skewed by structural categories | Noisy signals | Minimum-n, category allow-lists, configurable thresholds |
| Personal data (supplier CPFs) | Privacy/legal exposure | Hash CPFs from Silver onward; responsible-use doc |
| Local resource footprint (Trino + Dagster + MinIO) | Slow dev loop | Memory limits in Compose; fixture dataset for CI/dev |

## 7. Explicitly out of MVP (next increments)

- **V1:** Streaming (Redpanda) for CEAP incremental events; Senado ingestion; `cross_source_value_mismatch` (Transparência vs Tesouro amendments).
- **V2:** Investigation agent with typed tools (`resolve_entity`, `query_metric`, `compare_peers`, `get_source_record`, `search_signals`) on top of the semantic layer.
- **Later:** Receita CNPJ enrichment, PNCP contracts, IBGE context, graph edges, Explorer UI.
