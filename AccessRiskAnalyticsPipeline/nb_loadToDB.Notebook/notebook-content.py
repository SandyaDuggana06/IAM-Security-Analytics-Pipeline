# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "41bfecef-df72-44f2-8176-95ef243c32bf",
# META       "default_lakehouse_name": "lh_AccessRiskAnalyticsPipeline",
# META       "default_lakehouse_workspace_id": "7f37925a-9f89-47d7-88a9-860a1bf4cb93",
# META       "known_lakehouses": [
# META         {
# META           "id": "41bfecef-df72-44f2-8176-95ef243c32bf"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "5899b20d-f6e6-4dc1-9f09-7b00c3dadcb7",
# META       "known_warehouses": [
# META         {
# META           "id": "5899b20d-f6e6-4dc1-9f09-7b00c3dadcb7",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import monotonically_increasing_id, current_timestamp
import logging
import traceback
from time import time

start_time = time()

# =========================
# LOGGING SETUP
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("fabric_pipeline")


# =========================
# PIPELINE
# =========================
try:
    logger.info("=" * 60)
    logger.info("STARTING GOLD + FINAL LOAD LAYER")
    logger.info("=" * 60)

    # =====================
    # SILVER → FINAL TABLES
    # =====================
    logger.info("Loading Silver tables...")

    df_users = spark.table("silver_users")
    df_events = spark.table("silver_login_events")
    df_requests = spark.table("silver_access_requests")

    logger.info(f"Silver Users rows: {df_users.count()}")
    logger.info(f"Silver Events rows: {df_events.count()}")
    logger.info(f"Silver Requests rows: {df_requests.count()}")

    df_users.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("users")

    df_events.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("login_events")

    df_requests.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("access_requests")

    logger.info("Silver → Final tables loaded successfully")


    # =====================
    # GOLD → ANOMALIES
    # =====================
    logger.info("Processing Gold anomalies...")

    df_anomalies = spark.table("gold_anomalies")

    df_anomalies_final = (
        df_anomalies
        .select("user_id", "anomaly_type", "risk_level")
        .dropDuplicates(["user_id", "anomaly_type"])
        .withColumn("id", monotonically_increasing_id().cast("bigint"))
        .withColumn("detected_at", current_timestamp())
    )

    logger.info(f"Gold anomalies rows: {df_anomalies_final.count()}")

    df_anomalies_final.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("anomalies")

    logger.info("Gold anomalies loaded successfully")


    # =====================
    # PIPELINE SUMMARY
    # =====================
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)

    logger.info(f"Users table rows        : {spark.table('users').count()}")
    logger.info(f"Login events rows       : {spark.table('login_events').count()}")
    logger.info(f"Access requests rows    : {spark.table('access_requests').count()}")
    logger.info(f"Anomalies rows          : {spark.table('anomalies').count()}")

    logger.info(f"Total execution time: {round(time() - start_time, 2)} seconds")

    logger.info("PIPELINE COMPLETED SUCCESSFULLY ✅")


except Exception as e:
    logger.error("PIPELINE FAILED ❌")
    logger.error(str(e))
    logger.error(traceback.format_exc())
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
