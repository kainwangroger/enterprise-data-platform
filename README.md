# Enterprise Data Platform - Projet Portfolio Final (Data)
**Stack :** Kubernetes, Kafka, Spark, dbt, Airflow/Dagster, Terraform, Iceberg, Trino, Feast, DataHub, Prometheus, Grafana
**Volume :** Enterprise (2000 employes) | **Latence :** Hybride batch + streaming

## Comprendre le projet
### Contexte
En tant que Data Platform Engineer lead, le projet consiste a concevoir et implementer la plateforme data complete d'une entreprise de 2000 employes. Cette plateforme integre et depasse les 9 projets precedents en combinant ingestion multi-format (CDC, Kafka Connect, API Gateway), un Data Lakehouse en architecture Medallion (Bronze/Silver/Gold) avec Iceberg et Nessie, un Feature Store avec Feast, une gouvernance complete avec DataHub, et une observabilite SRE avec Prometheus, Grafana, Loki et Tempo.

## 1. Presentation & Specifications Metier

La plateforme repose sur une architecture Medallion avec 7 couches fonctionnelles :

**Infrastructure** : Kubernetes (K3s dev / EKS prod), Helm charts, Service Mesh (Istio), Vault pour les secrets, Terraform IaC.

**Ingestion Multi-format** : Change Data Capture (Debezium) pour les bases relationnelles, Kafka Connect pour les fichiers, REST API Gateway pour les webhooks, Schema Registry avec validation Avro/Protobuf.

**Data Lakehouse (Medallion)** : Bronze (donnees brutes Iceberg/Delta), Silver (donnees nettoyees, validees, dedupliquees), Gold (donnees agreegees modelisees dim+fact), Nessie catalog pour versionnement Git-like.

**Orchestration & Qualite** : Dagster/Airflow pour l'orchestration, Great Expectations pour la qualite, DataHub/OpenMetadata pour le lineage et glossaire.

**Self-Serve Analytics** : dbt pour les transformations, Trino pour le SQL ad-hoc, Metabase/Superset pour les dashboards, Jupyter avec Spark kernel.

**Feature Store & ML Serving** : Feast + Redis pour les features temps reel, MLflow pour le tracking et registry, API ML serving batch et temps reel.

**SRE & Observability** : Prometheus + Grafana pour les metriques par couche, Loki pour les logs centralises, Tempo pour le tracing distribue, PagerDuty/Slack alerts.

**FinOps & Governance** : Cost tracking par pipeline/equipe, Data classification (PII, sensitive, public), RBAC + column-level security, Audit logging.

## 2. Architecture Technique

```
  [Sources]
      API / DB / IoT / Events / Files
          |
  Bronze Layer (Ingestion)
  Kafka / Debezium CDC / NiFi / SFTP
  Schema Registry + AVRO / Protobuf
          |
  Silver Layer (Stream Processing)
  Flink / Spark Streaming
  CEP / Windows / Aggregations
  Feature Engineering temps reel
          |
  Silver Layer (Batch Processing)
  Spark / dbt / Airflow
  SCD2 / Data Quality (GE) / Lineage
          |
  Gold Layer (Storage)
  Iceberg / Delta / Hudi
  MinIO/S3 / Nessie (catalog)
  Trino / Presto (query engine)
          |
  Serving Layer
  Feature Store (Feast/Redis)
  FastAPI / GraphQL
  dbt models / Marts
          |
  Consumption
  Streamlit / Metabase / Superset
  MLflow / Jupyter / Airflow
```

## 3. Structure du Projet

```
.
  infra/
    kubernetes/
      kind-config.yaml
      k3s-config.yaml
    terraform/
      modules/
        k8s-cluster/
        kafka/
        spark/
        trino/
        feast/
        monitoring/
      environments/
        dev/
        staging/
        prod/
    helm/
      kafka/
      spark-operator/
      trino/
      feast/
      datahub/
  src/
    ingestion/
      kafka-connect/
        debezium-postgres.json
        s3-source.json
        jdbc-source.json
      api-gateway/
        webhook_handler.py
      schema-registry/
        schemas/
    streaming/
      flink-jobs/
        fraud_detection.py
        feature_engineering.py
      spark-streaming/
        cdc_to_iceberg.py
        enrichment.py
    batch/
      dagster/
        repository.py
        schedules.py
        sensors.py
      dbt/
        models/
          bronze/
          silver/
          gold/
        tests/
        macros/
    lakehouse/
      iceberg/
        table_definitions.sql
      nessie/
        branches.py
      trino/
        queries/
    serving/
      feast/
        feature_definitions.py
        feature_store.yaml
      fastapi/
        main.py
        routers/
        models/
      redis/
        cache_config.py
    quality/
      great_expectations/
        checkpoints/
        suites/
        plugins/
      datahub/
        ingestion/
        lineage/
    observability/
      prometheus/
        rules/
      grafana/
        dashboards/
      loki/
      tempo/
    governance/
      rbac/
        roles.yaml
        policies.rego
      classification/
        pii_detector.py
      audit/
        audit_logger.py
  docs/
    adrs/
      001-data-lakehouse.md
      002-ingestion-strategy.md
      003-orchestrator-choice.md
    runbooks/
      pipeline-failure.md
      kafka-outage.md
      data-quality-incident.md
    onboarding.md
```

## 4. Guide de Demarrage Rapide

```bash
# 1. Demarrer le Cluster Kubernetes Local (Kind/Minikube)
cd infra/kubernetes
kind create cluster --config kind-config.yaml

# 2. Deployer l'infrastructure Cloud avec Terraform (Optionnel)
cd infra/terraform/environments/dev
terraform init
terraform apply -auto-approve

# 3. Installer les charts Helm
helm repo add confluentinc https://confluentinc.github.io/helm-charts
helm install my-kafka confluentinc/cp-helm-charts

helm repo add trino https://trinodb.github.io/charts
helm install my-trino trino/trino

helm repo add feast https://feast-helm.github.io/charts
helm install my-feast feast/feast

# 4. Lancer les pipelines et l'orchestrateur
dagster dev -f src/batch/dagster/repository.py

# 5. Lancer l'API de serving
uvicorn src/serving/fastapi/main:app --reload --port 8000
```

## 5. Validation, Metriques & Observabilite

L'observabilite couvre chaque couche de la plateforme avec :
- **Layer 1 (Infra)** : CPU, memory, disk, network des noeuds K8s
- **Layer 2 (Ingestion)** : Kafka consumer lag, messages/sec, schema errors
- **Layer 3 (Streaming)** : Flink/Spark throughput, backpressure, checkpoint duration
- **Layer 4 (Batch)** : DAG duration, success rate, SLA breaches
- **Layer 5 (Storage)** : Iceberg snapshot size, compaction status, query performance
- **Layer 6 (Serving)** : API latency P50/P95/P99, error rate, cache hit ratio
- **Layer 7 (Governance)** : Lineage coverage, data quality score, cost per table

```bash
# Verifier le statut du cluster
kubectl get pods --all-namespaces

# Lancer les tests d'integration
pytest tests/integration/

# Verifier les metriques
curl http://localhost:9090/api/v1/query?query=up
```

**Documentation & Process :**
- ADRs (Architecture Decision Records) pour chaque choix technique dans `docs/adrs/`
- Runbooks pour les incidents communs dans `docs/runbooks/`
- Onboarding guide pour les nouveaux data engineers
- Post-mortem templates

## Skills Demonstrated

- Architecture Medallion (Bronze/Silver/Gold) avec Iceberg
- Infrastructure-as-Code (Terraform + Helm) sur Kubernetes
- Ingestion multi-format (CDC, Kafka Connect, API Gateway, Schema Registry)
- Orchestration batch et streaming (Dagster/Airflow + Flink/Spark)
- Self-Serve Analytics (dbt, Trino, Metabase, Jupyter)
- Feature Store (Feast + Redis) pour le ML serving
- Observabilite SRE (Prometheus, Grafana, Loki, Tempo)
- Data Governance (DataHub, Great Expectations, RBAC)
- FinOps et cost tracking par pipeline
- Service Mesh (Istio) et gestion des secrets (Vault)
- Resiliency : disaster recovery avec RPO/RTO < 1h
- Architecture Decision Records (ADRs) et runbooks
- Isolation des workloads (dev/staging/prod) sur le meme cluster
- Data sharing entre equipes sans couplage fort
- Gestion de 100+ pipelines avec SLA
