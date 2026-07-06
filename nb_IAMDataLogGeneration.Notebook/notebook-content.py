# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "431c2e1a-382c-492d-868f-4426caf771a3",
# META       "default_lakehouse_name": "LH_SecurityAnalyticsPipeline",
# META       "default_lakehouse_workspace_id": "7f37925a-9f89-47d7-88a9-860a1bf4cb93",
# META       "known_lakehouses": [
# META         {
# META           "id": "431c2e1a-382c-492d-868f-4426caf771a3"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "663ff195-c684-4527-933f-a462561b860c",
# META       "known_warehouses": [
# META         {
# META           "id": "663ff195-c684-4527-933f-a462561b860c",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import pandas as pd
import random
from datetime import datetime, timedelta

# -----------------------------
# Sample values
# -----------------------------
users = [f"user_{i}" for i in range(1, 51)]

roles = ["Admin", "Analyst", "Developer", "Manager"]
systems = ["HR_System", "Finance_App", "Trading_Platform", "Email_System"]
locations = ["Germany", "Switzerland", "India", "USA", "UK"]

def random_ip():
    return ".".join(str(random.randint(1, 255)) for _ in range(4))

# -----------------------------
# Generate data
# -----------------------------
data = []

for _ in range(5000):  # number of records
    user = random.choice(users)
    role = random.choice(roles)
    system = random.choice(systems)
    location = random.choice(locations)

    login_time = datetime.now() - timedelta(minutes=random.randint(0, 10000))

    status = random.choices(["success", "failed"], weights=[0.85, 0.15])[0]

    is_privileged = True if role == "Admin" else False

    data.append({
        "user_id": user,
        "role": role,
        "system": system,
        "location": location,
        "login_time": login_time,
        "status": status,
        "ip_address": random_ip(),
        "is_privileged": is_privileged
    })

df = pd.DataFrame(data)

# Save locally (optional)
df.to_csv("iam_logs.csv", index=False)

df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add suspicious activity: multiple failed logins
for i in range(100):
    df.loc[random.randint(0, len(df)-1), "status"] = "failed"

# Add unusual location for same user
df.loc[0:10, "location"] = "Unknown"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.__len__()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df=spark.createDataFrame(df)
spark_df.write.format("delta").mode('overwrite').saveAsTable("IAMLogs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
