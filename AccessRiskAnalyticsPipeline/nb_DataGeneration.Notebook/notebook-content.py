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

!pip install Faker

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# This notebook:
# - Generates synthetic enterprise security datasets
# - Saves raw datasets to the Lakehouse Files area
# - Loads raw data into Bronze Delta tables
# - Performs basic data quality validation
# - Logs pipeline execution statistics

# CELL ********************


#Welcome to your new notebook
# Type here in the cell editor to add code!
# Welcome to your new notebook
# Type here in the cell editor to add code!
import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import logging
from pyspark.sql.functions import current_timestamp
from time import time

start_time = time()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("fabric_pipeline")

fake = Faker()
random.seed(42)
np.random.seed(42)

# --- CONFIG ---
NUM_USERS = 2000
NUM_DAYS = 90

# --- DEPARTMENTS & ROLES ---
departments = {
    "Finance": ["Finance_Viewer", "Finance_Editor", "Finance_Admin"],
    "HR": ["HR_Viewer", "HR_Editor"],
    "IT": ["IT_Support", "IT_Admin", "Super_Admin"],
    "Engineering": ["Dev_Viewer", "Dev_Editor", "Dev_Admin"],
    "Operations": ["Ops_Viewer", "Ops_Editor"]
}

privileged_roles = ["Finance_Admin", "IT_Admin", "Super_Admin", "Dev_Admin"]

# --- GENERATE USERS ---
def generate_users():
    users = []
    for i in range(NUM_USERS):
        dept = random.choice(list(departments.keys()))
        role = random.choice(departments[dept])
        users.append({
            "user_id": f"USR{str(i+1).zfill(4)}",
            "username": fake.user_name(),
            "department": dept,
            "role": role,
            "is_privileged": role in privileged_roles,
            "account_status": random.choices(
                ["active", "inactive", "suspended"],
                weights=[80, 15, 5]
            )[0],
            "created_date": fake.date_between(start_date="-3y", end_date="-6m"),
            "last_password_change": fake.date_between(
                start_date="-1y", end_date="today"
            )
        })
    return pd.DataFrame(users)

# --- GENERATE LOGIN EVENTS ---
def generate_login_events(users_df):
    events = []
    start_date = datetime.now() - timedelta(days=NUM_DAYS)

    for _, user in users_df.iterrows():
        # Inactive/suspended users generate fewer events
        if user["account_status"] == "suspended":
            num_logins = random.randint(0, 3)  # anomaly: suspended users logging in
        elif user["account_status"] == "inactive":
            num_logins = random.randint(0, 10)
        else:
            num_logins = random.randint(20, 150)

        for _ in range(num_logins):
            login_time = start_date + timedelta(
                days=random.randint(0, NUM_DAYS),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )

            # Most logins during business hours, some anomalous after-hours
            is_after_hours = login_time.hour < 7 or login_time.hour > 19
            is_weekend = login_time.weekday() >= 5

            events.append({
                "event_id": fake.uuid4(),
                "user_id": user["user_id"],
                "login_time": login_time,
                "ip_address": fake.ipv4(),
                "country": random.choices(
                    ["Germany", "USA", "Unknown", "Russia", "China"],
                    weights=[70, 15, 5, 5, 5]
                )[0],
                "login_status": random.choices(
                    ["success", "failed"],
                    weights=[85, 15]
                )[0],
                "is_after_hours": is_after_hours,
                "is_weekend": is_weekend,
                "device_type": random.choice(["corporate_laptop", "personal_device", "mobile"])
            })

    return pd.DataFrame(events)

# --- GENERATE ACCESS REQUESTS ---
def generate_access_requests(users_df):
    requests = []
    for _ in range(500):
        user = users_df.sample(1).iloc[0]
        requests.append({
            "request_id": fake.uuid4(),
            "user_id": user["user_id"],
            "requested_role": random.choice(
                [r for roles in departments.values() for r in roles]
            ),
            "request_date": fake.date_between(start_date="-90d", end_date="today"),
            "status": random.choices(
                ["approved", "rejected", "pending"],
                weights=[60, 30, 10]
            )[0],
            "approver_id": f"USR{str(random.randint(1, 20)).zfill(4)}"
        })
    return pd.DataFrame(requests)


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting Bronze Layer - Data Generation Pipeline")
    logger.info("=" * 60)
# Generate Users
    logger.info("Generating Users dataset...")
    users = generate_users()
    users.to_csv("/lakehouse/default/Files/users.csv", index=False)
    #df_users_raw = spark.read.option("header", True).csv("/lakehouse/default/Files/users.csv") \
    #.withColumn("source_file", input_file_name()) \
    #.withColumn("ingested_at", current_timestamp())
    #df_users_raw.write.mode("overwrite").saveAsTable("bronze_users")
    df_users_raw = spark.createDataFrame(users) \
    .withColumn("ingested_at", current_timestamp())

    df_users_raw.write.mode("overwrite").saveAsTable("bronze_users")
    logger.info("bronze_users table created successfully.")
# Generate Login Events
    logger.info("Generating login events...")
    events = generate_login_events(users)
    events.to_csv("/lakehouse/default/Files/loginevents.csv", index=False)
    #df_events_raw = spark.read.option("header", True).csv("/lakehouse/default/Files/users.csv") \
    #.withColumn("source_file", input_file_name()) \
    #.withColumn("ingested_at", current_timestamp())
    #df_events_raw.write.mode("overwrite").saveAsTable("bronze_login_events")
    df_events_raw = spark.createDataFrame(events) \
    .withColumn("ingested_at", current_timestamp())

    df_events_raw.write.mode("overwrite").saveAsTable("bronze_login_events")
    logger.info("bronze_login_events table created successfully.")
# Generate Access Requests
    logger.info("Generating access requests...")
    requests = generate_access_requests(users)
    requests.to_csv("/lakehouse/default/Files/accessrequests.csv", index=False)
    #df_requests_raw = spark.read.option("header", True).csv("/lakehouse/default/Files/users.csv") \
    #.withColumn("source_file", input_file_name()) \
    #.withColumn("ingested_at", current_timestamp())

    #df_requests_raw.write.mode("overwrite").saveAsTable("bronze_access_requests")
    df_requests_raw = spark.createDataFrame(requests) \
    .withColumn("ingested_at", current_timestamp())

    df_requests_raw.write.mode("overwrite").saveAsTable("bronze_access_requests")
    logger.info("bronze_access_requests table created successfully.")
# Data Quality Checks
    assert df_users_raw.count() == len(users),"Users count mismatch!"
    assert df_users_raw.filter("user_id IS NULL").count() == 0, "Null user_id found!"
    logger.info("Users quality check passed.")
    assert df_events_raw.count() == len(events),"Login Events count mismatch!"
    assert df_events_raw.filter("event_id IS NULL").count() == 0, "Null event_id found!"
    logger.info("Login Events quality check passed.")
    assert df_requests_raw.count() == len(requests), "Access Requests count mismatch!"
    assert df_requests_raw.filter("request_id IS NULL").count() == 0, "Null request_id found!"
    logger.info("Access Requests quality check passed.")

# Pipeline Summary

    logger.info("========== PIPELINE SUMMARY ==========")

    logger.info(f"Users Generated            : {len(users)}")
    logger.info(f"Login Events Generated     : {len(events)}")
    logger.info(f"Access Requests Generated  : {len(requests)}")

    logger.info(f"bronze_users rows            : {df_users_raw.count()}")
    logger.info(f"bronze_login_events rows     : {df_events_raw.count()}")
    logger.info(f"bronze_access_requests rows  : {df_requests_raw.count()}")
    logger.info(f"Execution Time: {round(time() - start_time, 2)} seconds")

    logger.info("Bronze layer completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
