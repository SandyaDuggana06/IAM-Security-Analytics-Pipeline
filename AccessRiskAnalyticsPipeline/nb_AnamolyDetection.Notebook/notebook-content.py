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
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import pandas as pd
import logging
from time import time

start_time = time()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("fabric_pipeline")
logger.info("=" * 60)
logger.info("Starting Gold Layer - Anomaly Detection")
logger.info("=" * 60)

def detect_anomalies(users, events):
    anomalies = []

    # Merge users into events
    df = events.merge(users, on="user_id", how="left")

    # --- RULE 1: Suspended user logged in ---
    rule1 = df[
        (df["account_status"] == "suspended") &
        (df["login_status"] == "success")
    ].copy()
    rule1["anomaly_type"] = "Suspended Account Active Login"
    rule1["risk_level"] = "CRITICAL"

    logger.info(f"Rule 1 detected {len(rule1)} anomalies.")

    anomalies.append(rule1)

    # --- RULE 2: Login from unusual country ---
    rule2 = df[
        (df["country"].isin(["Russia", "China", "Unknown"])) &
        (df["login_status"] == "success")
    ].copy()
    rule2["anomaly_type"] = "Login from High-Risk Country"
    rule2["risk_level"] = "HIGH"

    logger.info(f"Rule 2 detected {len(rule2)} anomalies.")

    anomalies.append(rule2)

    # --- RULE 3: Privileged user logging in after hours ---
    rule3 = df[
        (df["is_privileged"] == True) &
        (df["is_after_hours"] == True) &
        (df["login_status"] == "success")
    ].copy()
    rule3["anomaly_type"] = "Privileged After-Hours Access"
    rule3["risk_level"] = "HIGH"

    logger.info(f"Rule 3 detected {len(rule3)} anomalies.")

    anomalies.append(rule3)

    # --- RULE 4: Multiple failed logins (brute force) ---
    failed_counts = df[df["login_status"] == "failed"].groupby(
        ["user_id", "date"]
    ).size().reset_index(name="failed_attempts")
    brute_force_users = failed_counts[failed_counts["failed_attempts"] >= 5]
    rule4 = df.merge(brute_force_users[["user_id", "date"]], on=["user_id", "date"])
    rule4["anomaly_type"] = "Possible Brute Force (5+ Failed Logins)"
    rule4["risk_level"] = "HIGH"

    logger.info(f"Rule 4 detected {len(rule4)} anomalies.")

    anomalies.append(rule4)

    # --- RULE 5: Login from personal device by privileged user ---
    rule5 = df[
        (df["is_privileged"] == True) &
        (df["device_type"] == "personal_device")
    ].copy()
    rule5["anomaly_type"] = "Privileged User on Personal Device"
    rule5["risk_level"] = "MEDIUM"

    logger.info(f"Rule 5 detected {len(rule5)} anomalies.")

    anomalies.append(rule5)

    # --- RULE 6: Stale password on active account ---
    rule6 = users[
        (users["password_stale"] == True) &
        (users["account_status"] == "active")
    ].copy()
    rule6["anomaly_type"] = "Active Account with Stale Password"
    rule6["risk_level"] = "MEDIUM"

    logger.info(f"Rule 6 detected {len(rule6)} anomalies.")

    anomalies.append(rule6)

    # Combine all anomalies
    result = pd.concat(anomalies, ignore_index=True)
    result = result.drop_duplicates()

    return result


if __name__ == "__main__":

    try:

        users = spark.table("silver_users").toPandas()
        events = spark.table("silver_login_events").toPandas()

        anomalies = detect_anomalies(users, events)

        assert len(anomalies) > 0, "No anomalies detected!"
        assert anomalies["anomaly_type"].isnull().sum() == 0
        assert anomalies["risk_level"].isnull().sum() == 0
        assert anomalies.duplicated().sum() == 0

        logger.info("Anomaly quality checks passed.")

        anomalies.to_csv("/lakehouse/default/Files/anomalies.csv", index=False)

        spark.createDataFrame(anomalies) \
            .write.mode("overwrite") \
            .saveAsTable("gold_anomalies")

        logger.info("gold_anomalies table created successfully.")

        assert spark.table("gold_anomalies").count() == len(anomalies)

        logger.info("Gold table verification successful.")

        logger.info("✅ Anomaly detection complete!")
        logger.info("Anomalies by Risk Level:")
        logger.info(anomalies["risk_level"].value_counts())

        logger.info("Anomalies by Type:")
        logger.info(anomalies["anomaly_type"].value_counts())
        logger.info("=" * 50)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 50)

        logger.info(f"Users Processed      : {len(users)}")
        logger.info(f"Events Processed     : {len(events)}")
        logger.info(f"Anomalies Detected   : {len(anomalies)}")
        logger.info(f"Execution Time       : {round(time() - start_time, 2)} seconds")

        logger.info("Gold Layer completed successfully.")

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise

    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
