from dagster import asset, Definitions, ScheduleDefinition, define_asset_job

@asset(description="Extraction et chargement des données brutes (Bronze Layer)")
def bronze_transactions_raw():
    """
    Simule la vérification de la présence de nouvelles données brutes dans la couche Bronze (S3/Iceberg).
    """
    print("Vérification des fichiers de transactions dans S3...")
    return {"status": "loaded", "rows_checked": 1000}

@asset(
    deps=[bronze_transactions_raw],
    description="Nettoyage et déduplication des données (Silver Layer)"
)
def silver_transactions_clean():
    """
    Simule un script de nettoyage (ex: filtrer les montants nuls, formater les dates, éliminer les doublons).
    """
    print("Nettoyage et standardisation des transactions...")
    return {"status": "cleaned", "valid_ratio": 0.98}

@asset(
    deps=[silver_transactions_clean],
    description="Agrégations analytiques et indicateurs métiers pour Trino (Gold Layer)"
)
def gold_kpis_daily():
    """
    Simule l'exécution de requêtes SQL complexes de transformation dbt sur la couche Gold
    pour calculer les volumes de vente quotidiens et les taux de fraude par pays.
    """
    print("Calcul des indicateurs financiers (volumes, pays, fraude)...")
    return {"status": "gold_ready", "kpi_metric_count": 15}

# Définition des jobs et schedules
gold_job = define_asset_job(name="medallion_pipeline_job", selection="*")

daily_schedule = ScheduleDefinition(
    job=gold_job,
    cron_schedule="0 2 * * *",  # Tous les jours à 2h du matin
)

defs = Definitions(
    assets=[bronze_transactions_raw, silver_transactions_clean, gold_kpis_daily],
    jobs=[gold_job],
    schedules=[daily_schedule]
)
