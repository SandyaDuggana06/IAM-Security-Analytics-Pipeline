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
df=spark.read.table("iamlogs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

list(df.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Checking for null values**

# CELL ********************

from pyspark.sql.functions import col,sum
df.select([
    sum(col(c).isNull().cast('int')).alias(c) for c in df.columns
    ]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
