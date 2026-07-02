# 10. Enterprise Data Platform — Projet Portfolio Final (Data)

**Stack :** Kubernetes, Kafka, Spark, dbt, Airflow/Dagster, Terraform, CI/CD, Iceberg, Trino, Feast, DataHub  
**Niveau :** Architecture (Staff/Principal) | **Contexte :** Plateforme complète pour 2000 employés

## Contexte Métier
Tu es le Data Platform Engineer lead. Tu conçois et implémentes la plateforme data complète d'une entreprise.

## Architecture (Medallion + Lambda Hybride)
```
Sources (API/DB/IoT/Files)
    → Bronze (Kafka/Debezium)
    → Silver (Flink/Spark/dbt)
    → Gold (Iceberg/Trino)
    → Serving (FastAPI/Feast)
    → Consumption (Streamlit/Metabase/MLflow)
```

## Ce que tu dois construire

### 1. Infrastructure (Terraform + Helm)
- Kubernetes, service mesh (Istio), Vault, cert-manager

### 2. Ingestion Multi-format
- CDC (Debezium), Kafka Connect, REST API Gateway

### 3. Data Lakehouse Medallion
- Bronze : données brutes Iceberg
- Silver : nettoyées, validées, dédupliquées
- Gold : agrégées, modélisées (dim + fact)

### 4. Orchestration & Qualité
- Dagster/Airflow, Great Expectations, DataHub

### 5. Self-Serve Analytics
- dbt transformations, Trino SQL, Metabase/Superset

### 6. Feature Store & ML Serving
- Feast + Redis, MLflow

### 7. SRE & Observability
- Prometheus + Grafana, Loki, Tempo (distributed tracing)

### 8. FinOps & Governance
- Cost tracking, data classification, RBAC, audit

## Structure attendue
```
infra/          # Terraform + Helm charts
src/
├── ingestion/  # Kafka Connect, Debezium, API Gateway
├── streaming/  # Flink/Spark jobs
├── batch/      # Airflow/Dagster DAGs
├── lakehouse/  # Iceberg tables + Nessie
├── serving/    # FastAPI, Feast, Redis
├── quality/    # Great Expectations
├── observability/ # Prometheus, Grafana, Loki
└── governance/ # DataHub, RBAC, audit
docs/
├── adrs/       # Architecture Decision Records
├── runbooks/   # Incident response
└── onboarding/ # Guide pour nouveaux engineers
```
