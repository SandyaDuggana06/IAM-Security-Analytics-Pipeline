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
import logging
import pandas as pd
import numpy as np
from time import time

start_time = time()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("fabric_pipeline")


def clean_users(df):
    logger.info("Cleaning users dataset")

    before = len(df)
    df = df.drop_duplicates(subset="user_id")
    logger.info("Removed %d duplicate users", before - len(df))

    df["department"] = df["department"].str.strip().str.title()
    df["account_status"] = df["account_status"].str.lower()

    df["created_date"] = pd.to_datetime(df["created_date"])
    df["last_password_change"] = pd.to_datetime(
        df["last_password_change"]
    )

    df["password_stale"] = (
        pd.Timestamp.now() - df["last_password_change"]
    ).dt.days > 90

    df["days_since_pwd_change"] = (
        pd.Timestamp.now() - df["last_password_change"]
    ).dt.days

    logger.info("Users cleaned successfully")
    return df


def clean_login_events(df):
    logger.info("Cleaning login events dataset")

    before = len(df)
    df = df.drop_duplicates(subset="event_id")
    logger.info("Removed %d duplicate login events", before - len(df))

    df["login_time"] = pd.to_datetime(df["login_time"])
    df["hour"] = df["login_time"].dt.hour
    df["day_of_week"] = df["login_time"].dt.day_name()
    df["date"] = df["login_time"].dt.date

    logger.info("Login events cleaned successfully")
    return df


def clean_access_requests(df):
    logger.info("Cleaning access requests dataset")

    before = len(df)
    df = df.drop_duplicates(subset="request_id")
    logger.info("Removed %d duplicate requests", before - len(df))

    df["request_date"] = pd.to_datetime(df["request_date"])

    logger.info("Access requests cleaned successfully")
    return df


if __name__ == "__main__":
    try:

        logger.info("=" * 60)
        logger.info("Starting Silver Layer - Data Cleaning Pipeline")
        logger.info("=" * 60)
        

    # users = clean_users(pd.read_csv("abfss://d92f3e05-045d-4358-9265-4c63ed74a012@onelake.dfs.fabric.microsoft.com/5eebf3b8-5ee2-4b28-bffa-c4f12fa9b3fe/Files/users.csv"))
        #events = clean_login_events(pd.read_csv("abfss://d92f3e05-045d-4358-9265-4c63ed74a012@onelake.dfs.fabric.microsoft.com/5eebf3b8-5ee2-4b28-bffa-c4f12fa9b3fe/Files/loginevents.csv"))
    # requests = clean_access_requests(
    #     pd.read_csv("abfss://d92f3e05-045d-4358-9265-4c63ed74a012@onelake.dfs.fabric.microsoft.com/5eebf3b8-5ee2-4b28-bffa-c4f12fa9b3fe/Files/accessrequests.csv")
    # )

        users = clean_users(spark.table("bronze_users").toPandas())
        assert users["user_id"].isnull().sum() == 0, "Null user_id found!"
        assert users["user_id"].duplicated().sum() == 0, "Duplicate user_id found!"
        assert users["account_status"].isin(["active", "inactive", "suspended"]).all(), "Invalid account status found!"
        logger.info("Users data quality checks passed.")

        events = clean_login_events(spark.table("bronze_login_events").toPandas())
        assert events["event_id"].isnull().sum() == 0, "Null event_id found!"
        assert events["event_id"].duplicated().sum() == 0, "Duplicate event_id found!"
        assert events["login_status"].isin(["success", "failed"]).all(), "Invalid login status found!"
        logger.info("Login Events data quality checks passed.")

        requests = clean_access_requests(spark.table("bronze_access_requests").toPandas())
        assert requests["request_id"].isnull().sum() == 0, "Null request_id found!"
        assert requests["request_id"].duplicated().sum() == 0, "Duplicate request_id found!"   
        assert requests["status"].isin(["approved", "rejected", "pending"]).all(), "Invalid request status found!"
        logger.info("Access Requests data quality checks passed.")

        users.to_csv("/lakehouse/default/Files/users_clean.csv",index=False)
        spark.createDataFrame(users).write.mode("overwrite").saveAsTable("silver_users")
        assert spark.table("silver_users").count() == len(users)
        logger.info("silver_users created successfully.")

        events.to_csv("/lakehouse/default/Files/login_events_clean.csv",index=False)
        spark.createDataFrame(events).write.mode("overwrite").saveAsTable("silver_login_events")
        assert spark.table("silver_login_events").count() == len(events)
        logger.info("silver_login_events created successfully.")

        requests.to_csv("/lakehouse/default/Files/access_requests_clean.csv",index=False)
        spark.createDataFrame(requests).write.mode("overwrite").saveAsTable("silver_access_requests")
        assert spark.table("silver_access_requests").count() == len(requests)
        logger.info("silver_access_requests created successfully.")

    

        logger.info(f"silver_users rows            : {len(users)}")
        logger.info(f"silver_login_events rows     : {len(events)}")
        logger.info(f"silver_access_requests rows  : {len(requests)}")
        logger.info(f"Active users: {(users['account_status'] == 'active').sum()}")
        logger.info(f"Successful logins: {(events['login_status'] == 'success').sum()}")
        logger.info(f"Approved requests: {(requests['status'] == 'approved').sum()}")
        logger.info("=" * 50)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 50)

        logger.info(f"Users cleaned           : {len(users)}")
        logger.info(f"Login Events cleaned    : {len(events)}")
        logger.info(f"Access Requests cleaned : {len(requests)}")

        logger.info("Silver tables created:")
        logger.info("- silver_users")
        logger.info("- silver_login_events")
        logger.info("- silver_access_requests")
        logger.info(f"Execution Time: {round(time() - start_time, 2)} seconds")

        logger.info("Silver Layer completed successfully.")

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
