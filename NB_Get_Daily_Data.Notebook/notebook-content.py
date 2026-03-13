# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3d3022ed-2651-4919-b627-8d42ddf1f2cf",
# META       "default_lakehouse_name": "LH_Wind_Power_Bronze",
# META       "default_lakehouse_workspace_id": "701df2d7-7e7b-408d-926d-8a0235da76a7",
# META       "known_lakehouses": [
# META         {
# META           "id": "3d3022ed-2651-4919-b627-8d42ddf1f2cf"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import requests
import pandas as pd
from datetime import timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Base URL for GitHub raw CSV files
base_url = "https://raw.githubusercontent.com/mikailaltundas/datasets-fortraining/main/wind-power-dataset/"
# Path to the wind_power table in the Bronze Lakehouse
bronze_table_path = "abfss://windPowerAnalytics@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Bronze.Lakehouse/Tables/dbo/wind_power_data"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load existing wind_power data and convert to Pandas
df_spark = spark.read.format("delta").load(bronze_table_path)
df_pandas = df_spark.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find the most recent date and calculate next day's date
most_recent_date = pd.to_datetime(df_pandas['date'], format ='%Y%m%d').max()
next_date = (most_recent_date + timedelta(days = 1)).strftime('%Y%m%d')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Download and load new data in a Pandas DataFrame
base_url = "https://raw.githubusercontent.com/mikailaltundas/datasets-for-training/main/wind-power-dataset/"
file_url = f"{base_url}{next_date}_wind_power_data.csv"
df_pandas_new = pd.read_csv(file_url)
df_pandas_new['date'] = pd.to_datetime(df_pandas_new['date'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert to Spark DataFrame and append in wind_power table
df_spark_new = spark.createDataFrame(df_pandas_new, schema =
df_spark.schema)
df_spark_new.write.format("delta").mode("append").save(bronze_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
