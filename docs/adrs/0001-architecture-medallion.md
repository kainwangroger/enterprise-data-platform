# Architecture Decision Record (ADR) 0001 : Choix de l'architecture Medallion Lakehouse

## Statut
Accepté

## Contexte
L'entreprise a besoin de traiter d'importants volumes de données transactionnelles à haute fréquence. Auparavant, les données étaient stockées dans des bases de données relationnelles traditionnelles et des fichiers bruts non structurés, ce qui limitait les performances analytiques et engendrait des risques importants de dégradation de la qualité des données.

## Décision
Nous adoptons une **architecture Lakehouse de type Medallion** s'appuyant sur les technologies suivantes :
- **Stockage sous format Apache Iceberg** pour bénéficier du support des transactions ACID, de l'évolution des schémas, et du Time-Travel.
- **Requêtage avec Apache Trino** en tant que moteur de requêtes SQL distribué ultra-rapide.
- **Découpage en 3 couches de stockage** :
  1. **Bronze (Raw)** : Ingestion brute en continu depuis Kafka sans transformation pour garantir la traçabilité des données d'origine.
  2. **Silver (Cleaned/Enriched)** : Nettoyage, standardisation et validation (avec Great Expectations).
  3. **Gold (Aggregated/Business)** : Tables dimensionnelles et de faits optimisées pour la BI et l'inférence Machine Learning.

## Conséquences
- **Avantages** :
  - Intégrité des données assurée par le support ACID.
  - Performance analytique accrue grâce à l'optimisation des requêtes Trino sur le format Iceberg.
  - Possibilité de revenir dans le passé (time-travel) pour auditer des transactions.
- **Inconvénients** :
  - Nécessité de maintenir un catalogue de métadonnées (comme Nessie ou un JDBC catalog) pour orchestrer les commits de transactions.
  - Complexité de configuration initiale des Helm charts sur Kubernetes.
