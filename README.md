# Enterprise Data Platform — Projet Portfolio Final (Data)

**Stack :** Kubernetes, Kafka, Spark, dbt, Airflow/Dagster, Terraform, Iceberg, Trino, Feast, DataHub, Prometheus, Grafana  
**Niveau :** Architecture (Staff/Principal) | **Contexte :** Plateforme complète pour 2000 employés

Ce projet intègre l'ensemble du cycle de vie des données d'entreprise dans une architecture moderne Medallion de type Lakehouse. Conçu pour être hautement résilient, scalable et sécurisé, le système est orchestré sur un cluster Kubernetes local et cloud.

## Architecture de la Plateforme (Lambda / Medallion)
```
Sources (API/DB/IoT/Files)
    │
    ▼
Bronze Layer (Kafka / Debezium / Ingestion bruts Iceberg)
    │
    ▼
Silver Layer (Flink / Spark Streaming / Transformations dbt / Qualité Great Expectations)
    │
    ▼
Gold Layer (Stockage Iceberg unifié / Requêtage Trino SQL)
    │
    ▼
Serving Layer (FastAPI / Feast Feature Store / Cache Redis)
    │
    ▼
Consumption Layer (Streamlit / Métabase BI / MLflow)
```

## Fonctionnalités
- **Infrastructure Infrastructure-as-Code** : Déploiement automatisé du cluster Kubernetes, du Service Mesh (Istio), de HashiCorp Vault pour les secrets et de Helm.
- **Ingestion Temps Réel & CDC** : Change Data Capture (CDC) en temps réel avec Debezium et Kafka Connect.
- **Data Lakehouse Structuré (Iceberg + Trino)** : Modélisation en couches Bronze/Silver/Gold avec support transactionnel ACID complet et gestion des schémas.
- **Gouvernance et Lignage de Données** : Portail de gouvernance avec DataHub, validation de qualité Great Expectations et gestion RBAC des accès.
- **SRE & Observabilité complète** : Surveillance centralisée des latences et logs avec Prometheus, Grafana, Loki et Tempo.

## Structure du Projet
```
├── infra/                  # Terraform + Helm charts
├── src/
│   ├── ingestion/          # Kafka Connect, Debezium, API Gateway
│   ├── streaming/          # Jobs Spark/Flink de transformation à la volée
│   ├── batch/              # Graphes d'orchestration Dagster/Airflow
│   ├── lakehouse/          # Définitions de tables Apache Iceberg + catalogue Nessie
│   ├── serving/            # FastAPI, Feast Feature Store, Redis
│   ├── quality/            # Règles de validation Great Expectations
│   ├── observability/      # Dashboards Prometheus et Grafana
│   └── governance/         # Lignage DataHub, configuration RBAC et audit
└── docs/
    ├── adrs/               # Architecture Decision Records (décisions d'architecture)
    └── runbooks/           # Guides opérationnels en cas d'incidents
```

## Prise en Main & Lancement

### 1. Démarrer le Cluster Kubernetes Local (Kind/Minikube)
```bash
cd infra/kubernetes
kind create cluster --config kind-config.yaml
```

### 2. Déployer l'infrastructure Cloud avec Terraform (Optionnel)
```bash
cd infra/terraform
terraform init
terraform apply -auto-approve
```

### 3. Installer les charts Helm (Kafka, Spark, Trino, Feast)
```bash
helm repo add confluentinc https://confluentinc.github.io/helm-charts
helm install my-kafka confluentinc/cp-helm-charts
```

### 4. Lancer les pipelines et l'orchestrateur
```bash
dagster dev -f src/batch/repository.py
```
