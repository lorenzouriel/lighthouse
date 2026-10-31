Yes. This can become a strong project if you define **Lighthouse as an analytical system**, not merely as a collection of Brazilian political datasets.

The core problem I would make it solve is:

> **Can we build a trustworthy, reproducible data platform that connects Brazilian public political data, detects inconsistencies and unusual patterns, and lets humans or AI agents investigate the evidence?**

That gives you a reason to use a lakehouse, streaming, a semantic layer, data quality, entity resolution, agents, and eventually an API/product.

## 1. Product definition

I would position it like this:

### **Lighthouse**
**Open intelligence infrastructure for Brazilian public and political data.**

Lighthouse continuously collects public datasets about politicians, elections, legislative activity, expenses, assets, contracts, companies, municipalities, economic indicators, and related entities.

It standardizes those sources into a unified lakehouse and exposes a semantic layer where analysts and AI agents can answer questions such as:

- How much has a politician spent on parliamentary expenses?
- Which suppliers receive unusually high amounts?
- Did a politician's declared assets change significantly between elections?
- Do payments reported by different public sources disagree?
- Which politicians share suppliers?
- Which suppliers receive money from many politicians?
- Are there expenses substantially above peers for the same category?
- What companies are connected to a politician or supplier?
- How did a municipality receiving federal transfers change economically?
- Which votes contradict a politician's stated policy position?
- Where does a value shown by one government dataset disagree with another dataset?

The important distinction is:

> Lighthouse should **surface evidence and inconsistencies**, not declare that someone committed wrongdoing.

That makes the project considerably more defensible and analytically useful.

---

# 2. Your current sources already fit the concept

The Base dos Dados dataset you linked contains Senado/Congresso legislative data including **senators, nominal votes, legislative processes, committees, parties, blocs, leadership and the Mesa Diretora**, with coverage extending from 1935 through October 29, 2026. [Base dos Dados](https://basedosdados.org/dataset/5c5506e8-d190-4b47-9259-caf293ca5fc5?table=5fa27f7f-edf1-4192-92fb-99357b322028)

The Câmara also exposes detailed datasets and APIs around deputies, expenses, speeches, events, parties, propositions, legislative bodies and other entities. Importantly for Lighthouse, the parliamentary expense dataset goes back to **2008** and is updated daily. [Dados Abertos Câmara](https://dadosabertos.camara.leg.br/swagger/api.html?utm_source=chatgpt.com)

TSE is another high-value source. Its candidate datasets include candidate profiles, **declared assets**, parties/coalitions, social networks and related election information. [Portal de Dados Abertos do TSE](https://dadosabertos.tse.jus.br/group/candidatos?utm_source=chatgpt.com)

And your Observatório example illustrates another useful pattern: enriching political datasets with **economic indicators, historical series and territorial/sectoral context** rather than considering political data in isolation. [Comércio Insights](https://observatorio.portaldocomercio.org.br/?utm_source=chatgpt.com)

That leads to a much more interesting graph:

```text
Politician
   │
   ├──── belongs_to ──── Party
   │
   ├──── elected_in ──── Election
   │
   ├──── represents ───── State
   │
   ├──── declared ─────── Asset
   │
   ├──── received ─────── Donation
   │
   ├──── incurred ─────── Expense
   │                       │
   │                       └──── paid_to ─── Supplier
   │
   ├──── voted_on ──────── Proposition
   │
   ├──── member_of ─────── Committee
   │
   └──── associated_with ─ Company

Supplier / Company
   │
   ├──── located_in ───── Municipality
   │
   └──── receives ─────── Public Money

Municipality
   │
   ├──── GDP
   ├──── population
   ├──── employment
   ├──── transfers
   └──── elections
```

Now you have a **political knowledge graph backed by a lakehouse**.

---

# 3. The architecture I would use

Your architecture can legitimately combine:

```text
                 LIGHTHOUSE

           Sources / Open Data
                    │
       ┌────────────┴─────────────┐
       │                          │
   Batch sources             Live sources
 CSV / Parquet / APIs       APIs / events / feeds
       │                          │
       ▼                          ▼
   Ingestion                  Streaming
   Airbyte/dlt              Kafka / Redpanda
       │                          │
       └────────────┬─────────────┘
                    ▼
             ┌─────────────┐
             │  LAKEHOUSE  │
             │ Iceberg     │
             │ S3/MinIO    │
             └──────┬──────┘
                    │
          ┌─────────┴──────────┐
          ▼                    ▼
       Bronze                Silver
       raw data          standardized data
                                  │
                                  ▼
                                Gold
                         analytical entities
                                  │
                    ┌─────────────┼─────────────┐
                    │             │             │
                    ▼             ▼             ▼
                 dbt          Semantic       Graph
               metrics         Layer         Model
                    │             │             │
                    └──────┬──────┴─────────────┘
                           ▼
                        Agents
                           │
                    investigation
                           │
                           ▼
                  Evidence / Answers
```

This is not architecture for architecture's sake. Every layer has a clear purpose.

---

# 4. Lakehouse

For the lakehouse, I'd strongly consider:

```text
Object Storage
     │
     └── S3 / MinIO
             │
          Iceberg
             │
      ┌──────┴──────┐
      │             │
   DuckDB          Spark
 development     large workloads
      │
      └────── Trino
              │
             dbt
```

For a portfolio project, a very good stack would be:

| Layer | Technology |
|---|---|
| Storage | S3 / MinIO |
| Table format | Apache Iceberg |
| Catalog | Iceberg REST Catalog / Nessie |
| Transformation | dbt |
| Query | DuckDB + Trino |
| Larger processing | Spark |
| Orchestration | Dagster |
| Streaming | Redpanda/Kafka |
| Quality | Soda / Great Expectations / dbt tests |
| API | FastAPI |
| Metadata | OpenMetadata/DataHub later |
| Agents | Python + tool-calling LLM |
| Semantic definitions | dbt Semantic Layer / MetricFlow or custom YAML |

You don't need every component on day one.

---

# 5. The medallion model makes sense here

## Bronze — Evidence preservation

Keep public datasets essentially untouched.

```text
bronze/
├── camara/
│   ├── deputados/
│   ├── despesas/
│   └── votacoes/
├── senado/
├── tse/
│   ├── candidatos/
│   ├── bens/
│   └── prestacao_contas/
├── transparencia/
├── ibge/
└── receita/
```

Every record should carry metadata such as:

```text
_source
_source_url
_ingestion_timestamp
_source_updated_at
_checksum
_schema_version
```

This matters enormously for a project about public accountability.

You want to be able to answer:

> "Where exactly did this number come from?"

---

# 6. Silver — solve the difficult problem

The difficult part isn't downloading CSV files.

It's **entity resolution**.

Different government datasets may represent the same person as:

```text
JOAO DA SILVA
João da Silva
JOÃO SILVA
Joao P. da Silva
JOÃO PEDRO DA SILVA
```

Companies are even worse.

You therefore need canonical entities.

Example:

```text
dim_politician

politician_id
cpf_hash
canonical_name
birth_date
state
party
tse_candidate_id
camara_id
senado_id
```

Then:

```text
entity_alias

source
source_entity_id
politician_id
source_name
confidence
resolution_method
```

For example:

```text
TSE: 123456
        │
        ▼
 politician_9342
        ▲
        │
CAMARA: 204554
```

This is one of the most impressive parts you could demonstrate in a Data Engineering portfolio.

---

# 7. Gold — analytical models

Your Gold layer should be opinionated.

Examples:

```text
gold.politician
gold.party
gold.election
gold.asset
gold.expense
gold.supplier
gold.company
gold.vote
gold.proposition
gold.municipality
gold.donation
```

And aggregates:

```text
gold.politician_expense_monthly
gold.politician_asset_history
gold.supplier_political_payments
gold.party_expense_summary
gold.expense_peer_benchmark
gold.politician_voting_profile
```

This is where Lighthouse becomes reusable.

---

# 8. The most valuable feature: inconsistency detection

I'd make this a first-class domain.

Instead of simply saying:

```text
expense = R$ 12,300
```

Lighthouse could calculate:

```text
expense:
    value: 12,300

peer_median:
    value: 2,100

percentile:
    99.2%

historical_average_for_politician:
    1,850

increase:
    +565%

supplier_frequency:
    first transaction

signal:
    unusual_expense
```

That is far more useful.

---

# 9. Don't call everything "fraud detection"

Use a hierarchy like:

```text
Observation
    │
    ▼
Anomaly
    │
    ▼
Inconsistency
    │
    ▼
Investigation Signal
    │
    ▼
Human Review
```

Not:

```text
Anomaly → Fraud
```

An anomaly is evidence worth examining, not evidence of a crime.

This distinction should be built directly into the product.

---

# 10. Anomaly engine

You could have rule-based signals first.

### Example — expense anomaly

```python
expense.value > peer_p95
```

### Repeated supplier

```text
politician
     │
     ├── R$ 32k ── supplier X
     ├── R$ 41k ── supplier X
     ├── R$ 29k ── supplier X
     └── R$ 38k ── supplier X
```

### Shared supplier

```text
Politician A ──┐
Politician B ──┼── Supplier X
Politician C ──┤
Politician D ──┘
```

### Asset jump

```text
2018: R$ 430k
2022: R$ 1.1M
2026: R$ 4.9M
```

Lighthouse flags:

```text
asset_growth = +345%
```

Then contextualizes rather than concludes.

### Cross-source mismatch

```text
Source A
R$ 182,500

Source B
R$ 179,800

Difference
R$ 2,700
```

Now the agent can investigate why.

This particular capability fits your original idea extremely well.

---

# 11. Streaming actually has a legitimate role

A common portfolio mistake is using Kafka where Kafka isn't needed.

Here it can make sense.

Imagine an ingestion event:

```json
{
  "event": "expense.updated",
  "source": "camara",
  "politician_id": "br-politician-9328",
  "expense_id": "xxx",
  "value": 14800
}
```

Then:

```text
Government API
      │
      ▼
 ingestion job
      │
      ▼
    Kafka
      │
      ├──► lakehouse writer
      │
      ├──► quality monitor
      │
      ├──► anomaly detector
      │
      └──► notification service
```

You can therefore demonstrate **Lambda/Kappa-like concepts without creating fake streaming requirements**.

---

# 12. What exactly should be streamed?

Not everything.

I would distinguish:

### Batch

```text
historical elections
old assets
old expenses
IBGE data
historical economic indicators
company datasets
```

### Incremental / near-real-time

```text
new parliamentary expenses
new votes
new legislative proposals
candidate updates during elections
government transfers
new contracts
```

Your architecture becomes:

```text
                ┌──── Batch ─────┐
Sources ────────┤                ├── Lakehouse
                └── Streaming ───┘
```

That is much more credible than forcing Kafka everywhere.

---

# 13. The Semantic Layer

This is where your AI idea becomes interesting.

Do **not** let the agent invent SQL over hundreds of raw tables.

Give it a semantic model.

For example:

```yaml
entity: politician

dimensions:
  - name
  - state
  - party
  - legislature

metrics:
  total_expenses:
    measure: sum(expense.value)

  avg_monthly_expense:
    measure: avg(monthly_expense)

  declared_assets:
    measure: sum(asset.value)

relationships:
  expenses: one_to_many
  assets: one_to_many
  votes: one_to_many
  party: many_to_one
```

Now the agent understands concepts instead of storage internals.

---

# 14. AI agents should be investigators, not chat-with-SQL wrappers

This is important.

A weak implementation:

```text
User
 ↓
LLM
 ↓
SQL
 ↓
Answer
```

A much better Lighthouse design:

```text
User question
      │
      ▼
Investigation Agent
      │
      ├── semantic query
      ├── anomaly search
      ├── entity resolution
      ├── source verification
      ├── historical comparison
      └── cross-source comparison
      │
      ▼
Evidence
      │
      ▼
Explanation
```

Example question:

> "Did Deputy X have unusual transportation expenses in 2026?"

The agent might execute:

```text
1. Resolve "Deputy X"
2. Retrieve expenses
3. Filter transportation
4. Compare against:
   - politician history
   - party peers
   - same state
   - all deputies
5. identify anomalies
6. retrieve underlying records
7. generate explanation
```

Answer:

```text
Transportation expenses: R$ 187,200

Compared with:
same-state median: R$ 62,300
national median: R$ 58,900

Percentile: 97.8

Largest contributing supplier:
Empresa X — R$ 74,100

Evidence:
Camara CEAP records (...)
```

That is a genuinely useful agent.

---

# 15. Add a provenance system

Every result should have lineage.

For example:

```text
Finding
  │
  ├── metric
  │     └── gold.politician_expenses
  │
  ├── transformation
  │     └── dbt model
  │
  ├── normalized rows
  │     └── silver.camara_expenses
  │
  └── original evidence
        └── Câmara API
```

Expose something like:

```json
{
  "claim": "Transport expenses increased 212%",
  "sources": [
    {
      "dataset": "Câmara CEAP",
      "records": [...]
    }
  ],
  "calculation": "...",
  "generated_at": "...",
  "model_version": "..."
}
```

Now Lighthouse's key property becomes:

> **Every answer is traceable back to evidence.**

That is a very strong project principle.

---

# 16. Give Lighthouse a canonical identifier system

For example:

```text
br:politician:1234
br:company:33000167000101
br:party:pt
br:municipality:3550308
br:election:2026
br:proposition:pl-1234-2026
```

Then all sources converge onto those identifiers.

This enables:

```text
GET /politicians/br:politician:1234
```

And later:

```text
GET /politicians/{id}/expenses
GET /politicians/{id}/assets
GET /politicians/{id}/votes
GET /politicians/{id}/signals
GET /companies/{id}/political-payments
```

You have effectively created a **Brazilian political data API**.

That has service potential.

---

# 17. The product surface

Eventually I can see four Lighthouse products.

```text
                    LIGHTHOUSE

                         │
        ┌────────────────┼─────────────────┐
        │                │                 │
        ▼                ▼                 ▼

 Lighthouse Data   Lighthouse Signals   Lighthouse Agent
      API              Engine              

        │
        └────────────────┬─────────────────┘
                         ▼

                  Lighthouse Explorer
```

### Lighthouse Data

Normalized political datasets.

Potential consumers:

```text
journalists
researchers
universities
NGOs
developers
data scientists
```

### Lighthouse Signals

Programmatically detected anomalies/inconsistencies.

```text
expense_outlier
asset_jump
supplier_concentration
cross_source_mismatch
unusual_transaction
```

### Lighthouse Agent

Natural language investigation.

> "Show deputies from SP with expense growth above 100%."

### Lighthouse Explorer

Public interface.

```text
/search?q=Politician Name
```

Then:

```text
Profile
Expenses
Assets
Votes
Connections
Signals
Sources
```

---

# 18. One particularly strong feature: politician timeline

Each politician could have:

```text
                          POLITICIAN

2018                    2022                    2026
 │                        │                       │
 Election                 Election                Election
 │                        │                       │
 Assets R$500k           Assets R$900k           Assets R$2.4m
 │                        │                       │
 ├──── Expense           ├──── Expense           ├──── Expense
 ├──── Vote              ├──── Vote              ├──── Vote
 ├──── Party change      ├──── Contract          ├──── Signal
 │                        │                       │
 ▼                        ▼                       ▼
```

Now political data becomes longitudinal rather than isolated tables.

---

# 19. Graph analytics becomes very useful later

Once your canonical entities exist, you can construct:

```text
              Supplier A
              /       \
             /         \
       Politician A   Politician B
             \         /
              \       /
               Party X
```

Then graph algorithms could detect:

- highly connected suppliers;
- unusual communities;
- repeated relationships;
- central companies;
- politically concentrated vendors.

You don't even need Neo4j initially.

You could generate edges from Iceberg:

```text
from_id
relation
to_id
valid_from
valid_to
source
```

Later load those edges into something such as Neo4j.

---

# 20. Data sources I would progressively add

I'd roughly use this progression:

| Source | What it gives Lighthouse |
|---|---|
| Câmara | deputies, expenses, votes, propositions |
| Senado | senators, votes, legislative data |
| TSE | elections, candidates, assets, campaign finance |
| IBGE | socioeconomic context |
| Banco Central | macroeconomic context |
| Tesouro | government financial data |
| Portal da Transparência | transfers/contracts/spending |
| Receita Federal open datasets | company/entity information where available |
| PNCP | public procurement |
| Base dos Dados | standardized access to many public datasets |
| state open-data portals | subnational politics |
| municipal portals | local politics |

This gives you a path from **political activity → money → companies → geography → socioeconomic context**.

---

# 21. A use case that could become the project's flagship demo

I would call it:

## **Follow the Money**

User asks:

> Where did Deputy X's parliamentary allowance go in 2026?

Lighthouse produces:

```text
Total expenses
R$ 418,300

By category
Advertising       R$ 121,000
Transport         R$ 105,000
Office services    R$ 82,000
...

Top suppliers
Company A          R$ 84,200
Company B          R$ 64,100
Company C          R$ 53,500

Signals
⚠ Company A represents 20.1% of expenditure
⚠ Advertising expense > 97th percentile
⚠ Supplier shared with 14 other politicians
```

Then:

> Investigate Company A.

Agent performs:

```text
Company
   ↓
payments
   ↓
politicians
   ↓
parties
   ↓
geography
   ↓
historical payments
```

This showcases virtually your entire stack.

---

# 22. Another flagship: cross-source reconciliation

This is even closer to what you originally described.

Imagine:

```text
                  Lighthouse Reconciliation

Câmara ───────────┐
                  │
TSE ──────────────┤
                  ├── Entity Resolution
Transparency ─────┤          │
                  │          ▼
PNCP ─────────────┘     Canonical Facts
                             │
                             ▼
                        Comparison Engine
                             │
                   ┌─────────┴─────────┐
                   │                   │
                matches             mismatch
```

Example:

```text
Expected value       R$ 104,532.21
Reported source A    R$ 104,532.21
Reported source B    R$ 109,532.21

Difference           R$ 5,000
```

Then store:

```text
signal_type:
    cross_source_value_mismatch

severity:
    medium

evidence:
    source_a
    source_b
```

This is a very legitimate real-world Data Engineering problem.

---

# 23. Your repository could look like this

```text
lighthouse/
│
├── README.md
├── docs/
│   ├── architecture.md
│   ├── data-model.md
│   ├── semantic-layer.md
│   └── data-sources.md
│
├── ingestion/
│   ├── camara/
│   ├── senado/
│   ├── tse/
│   └── ibge/
│
├── streaming/
│   ├── producers/
│   └── consumers/
│
├── lakehouse/
│   ├── schemas/
│   └── migrations/
│
├── transformations/
│   └── dbt/
│
├── semantic/
│   ├── entities/
│   ├── metrics/
│   └── relationships/
│
├── signals/
│   ├── anomaly_detection/
│   ├── reconciliation/
│   └── rules/
│
├── agents/
│   ├── investigator/
│   ├── query/
│   └── evidence/
│
├── api/
│
├── infrastructure/
│   ├── terraform/
│   └── docker/
│
└── tests/
```

That repository itself tells a strong engineering story.

---

# 24. What I would **not** build initially

Don't start with:

- 30 datasets;
- Neo4j;
- Kubernetes;
- Spark clusters;
- five agents;
- a complex frontend;
- ML anomaly detection;
- a full chatbot.

You will spend months assembling infrastructure without producing an interesting result.

Instead, make one **vertical slice** work.

---

# 25. Lighthouse V0

I'd start with only:

```text
Câmara
+
TSE
```

And three entities:

```text
Politician
Expense
Supplier
```

plus:

```text
Election
Asset
```

Then solve these five questions:

1. **How much did politician X spend?**
2. **Who received that money?**
3. **Is the spending unusual relative to peers?**
4. **How have the politician's declared assets changed?**
5. **What evidence supports those answers?**

If Lighthouse answers those five questions reliably, you already have a strong project.

---

# 26. Then introduce streaming

V1:

```text
daily ingestion
      │
      ▼
new expense
      │
      ▼
Kafka
      │
      ▼
Iceberg
      │
      ▼
signal calculation
```

Example event:

```text
Expense detected
        │
        ▼
Supplier resolution
        │
        ▼
Metric update
        │
        ▼
Anomaly calculation
        │
        ▼
Signal created
```

Now your portfolio demonstrates:

- CDC/incremental concepts;
- event architecture;
- exactly-once/idempotency concerns;
- event schema evolution;
- replay;
- lakehouse integration.

---

# 27. Then introduce the agent

Only after your semantic layer is stable.

The agent should have tools like:

```text
resolve_entity()
query_metric()
find_expenses()
compare_peers()
find_relationships()
get_source_record()
search_signals()
```

Not:

```text
execute_arbitrary_sql()
```

This makes it much safer and easier to evaluate.

---

# 28. Your semantic layer becomes the real intellectual property

The lakehouse is infrastructure.

The valuable thing is something like:

```text
politician.total_expenses
politician.declared_assets
politician.asset_growth
politician.expense_percentile
politician.supplier_concentration
politician.party_changes
politician.voting_alignment

supplier.total_public_payments
supplier.politician_count
supplier.party_concentration
supplier.payment_growth
```

These concepts can become an API, research dataset, MCP server, or agent toolset.

That is where Lighthouse can eventually become a **service** rather than remaining a portfolio repository.

---

# 29. What makes Lighthouse different from Base dos Dados

You shouldn't attempt to make:

> "Base dos Dados but smaller."

Base dos Dados is already doing excellent work standardizing datasets.

Your layer should be above that:

```text
                    Raw government data
                           │
                    Base dos Dados
                           │
                           ▼
                    standardized data
                           │
                           ▼
                       LIGHTHOUSE
                           │
                political semantics
                           │
                     entity graph
                           │
                      metrics
                           │
                     anomalies
                           │
                  investigations
```

Base dos Dados answers:

> "Where is the dataset?"

Lighthouse should answer:

> **"What does the evidence across datasets tell us?"**

That is a much stronger differentiation.

---

# 30. The one-sentence architecture thesis

I would put something close to this in the README:

> **Lighthouse is an open lakehouse and streaming intelligence platform that unifies Brazilian public political data into traceable entities, metrics and relationships, allowing humans and AI agents to investigate spending, assets, voting behavior and inconsistencies across official sources.**

And the technical thesis:

```text
Open Data
   ↓
Batch + Streaming
   ↓
Iceberg Lakehouse
   ↓
Canonical Entities
   ↓
Semantic Layer
   ↓
Metrics + Signals
   ↓
Agentic Investigation
   ↓
Evidence-backed answers
```

That gives you a project that simultaneously demonstrates **modern data engineering, lakehouse architecture, event streaming, data modeling, entity resolution, data quality, semantic modeling, analytics engineering and practical AI agents**—while producing something with clear reuse potential rather than merely showcasing technologies.

Would you like to shape this into a concrete V0 architecture and repository plan next?