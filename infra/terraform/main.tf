terraform {
  required_version = ">= 1.3.0"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.20.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.9.0"
    }
  }
}

provider "kubernetes" {
  config_path = "~/.kube/config"
}

provider "helm" {
  kubernetes {
    config_path = "~/.kube/config"
  }
}

# Namespace for our Enterprise Data Platform
resource "kubernetes_namespace" "edp_namespace" {
  metadata {
    name = "enterprise-data-platform"
    labels = {
      environment = "production"
      owner       = "data-platform-team"
    }
  }
}

# Helm deployment of Apache Kafka (Confluent Schema Registry & Broker)
resource "helm_release" "kafka_confluent" {
  name       = "kafka-cluster"
  repository = "https://confluentinc.github.io/helm-charts"
  chart      = "cp-helm-charts"
  namespace  = kubernetes_namespace.edp_namespace.metadata[0].name
  version    = "0.6.0"

  values = [
    <<-EOT
    cp-zookeeper:
      enabled: false
    cp-kafka:
      brokers: 3
      configurationOverrides:
        "offsets.topic.replication.factor": "3"
        "default.replication.factor": "3"
        "min.insync.replicas": "2"
    EOT
  ]
}

# Helm deployment of Apache Trino
resource "helm_release" "trino" {
  name       = "trino-query-engine"
  repository = "https://trinodb.github.io/charts"
  chart      = "trino"
  namespace  = kubernetes_namespace.edp_namespace.metadata[0].name

  values = [
    file("${path.module}/../helm/values-trino.yaml")
  ]
}
