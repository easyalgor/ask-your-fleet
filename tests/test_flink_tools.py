import os
import sys
import time

# Ensure the project root is on sys.path so sibling modules resolve
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

from tools import (
    get_latest_device_health,
    get_device_trend,
    get_anomalies,
    list_flink_tables,
    describe_table,
    run_flink_sql,
    read_kafka_topic,
    create_custom_data_product,
    insert_to_data_product,
    delete_data_product,
)

print("\n--- 1. LIST TABLES ---")
print(list_flink_tables())

print("\n--- 2. DESCRIBE SOURCE TABLE ---")
print(describe_table("device_health_5min"))

print("\n--- 3. LATEST HEALTH (MATERIALIZED VIEW FLOW) ---")
# This internally creates a topic and starts an ingestion job
print(get_latest_device_health("sensor-26"))

print("\n--- 4. READ KAFKA TOPIC DIRECTLY ---")
# Reading from the topic created in step 3
print(read_kafka_topic("dp_health_sensor_26", 5))

print("\n--- 5. CREATE CUSTOM DATA PRODUCT (SCHEMA ONLY) ---")
# Using a primary key to support changelog/upsert streams
schema = "deviceId STRING, window_start TIMESTAMP(3), avg_temp DOUBLE, PRIMARY KEY (deviceId, window_start) NOT ENFORCED"
select = "SELECT deviceId, window_start, avg_temp FROM `kt_environment`.`fms-tableflow-demo`.`device_health_5min` WHERE avg_temp > 50.0"
print(create_custom_data_product("dp_high_temp_devices", schema, select))

print("\n--- 6. STANDALONE INSERT INTO DATA PRODUCT ---")
# Demonstrating the new standalone tool
select_more = "SELECT deviceId, window_start, avg_temp FROM `kt_environment`.`fms-tableflow-demo`.`device_health_5min` WHERE avg_temp < 20.0"
print(insert_to_data_product("dp_high_temp_devices", select_more))

print("\n--- 7. READ CUSTOM DATA PRODUCT ---")
time.sleep(5)
print(read_kafka_topic("dp_high_temp_devices", 10))

print("\n--- 8. DELETE DATA PRODUCTS ---")
print(delete_data_product("dp_health_sensor_26"))
print(delete_data_product("dp_high_temp_devices"))