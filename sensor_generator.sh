#!/bin/bash
# ──────────────────────────────────────────────────────────────
# IoT Sensor Data Generator — publishes simulated telemetry
# to an MQTT broker (e.g. HiveMQ Cloud) for demo purposes.
#
# Prerequisites:
#   brew install mosquitto       # macOS
#   sudo apt install mosquitto-clients  # Linux
#
# Usage:
#   cp .env.mqtt.example .env.mqtt   # fill in your broker creds
#   source .env.mqtt
#   ./sensor_generator.sh
# ──────────────────────────────────────────────────────────────

set -euo pipefail

# -------------------------------
# CONFIG — set via environment variables
# -------------------------------
MINVAL=200
MAXVAL=800

PUBEXE="${MQTT_PUB_CMD:-mosquitto_pub}"

HOST="${MQTT_HOST:?Set MQTT_HOST (e.g. your-broker.hivemq.cloud)}"
PORT="${MQTT_PORT:-8883}"
TOPIC="${MQTT_TOPIC:-temperature}"
QOS="${MQTT_QOS:-1}"

USERNAME="${MQTT_USERNAME:?Set MQTT_USERNAME}"
PASSWORD="${MQTT_PASSWORD:?Set MQTT_PASSWORD}"

TLS="${MQTT_TLS_FLAGS:---insecure -V mqttv311}"

DEVICE_COUNT="${DEVICE_COUNT:-50}"

# -------------------------------
# ARRAYS (macOS compatible)
# -------------------------------
TEMP_STATE=()
PRESSURE_STATE=()
VIBRATION_STATE=()

# -------------------------------
# INIT DEVICES
# -------------------------------
initDevices() {
  for ((i=0;i<DEVICE_COUNT;i++)); do
    TEMP_STATE[$i]=$((RANDOM % (MAXVAL - MINVAL) + MINVAL))
    PRESSURE_STATE[$i]=$((RANDOM % 200 + 900))
    VIBRATION_STATE[$i]=$((RANDOM % 50))
  done
}

# -------------------------------
# VALUE UPDATE
# -------------------------------
updateValue() {
  local val=$1
  local min=$2
  local max=$3

  delta=$((RANDOM % 20 - 10))
  val=$((val + delta))

  if [ $val -lt $min ]; then val=$min; fi
  if [ $val -gt $max ]; then val=$max; fi

  echo $val
}

# -------------------------------
# GENERATE EVENT
# -------------------------------
generateEvent() {
  local idx=$1

  TEMP_STATE[$idx]=$(updateValue ${TEMP_STATE[$idx]} $MINVAL $MAXVAL)
  PRESSURE_STATE[$idx]=$(updateValue ${PRESSURE_STATE[$idx]} 800 1200)
  VIBRATION_STATE[$idx]=$(updateValue ${VIBRATION_STATE[$idx]} 0 100)

  local temp=${TEMP_STATE[$idx]}
  local pressure=${PRESSURE_STATE[$idx]}
  local vibration=${VIBRATION_STATE[$idx]}

  status="NORMAL"

  # 🔥 anomaly injection (random spikes)
  if [ $((RANDOM % 30)) -eq 0 ]; then
    temp=$((900 + RANDOM % 100))
    vibration=$((800 + RANDOM % 200))
    pressure=$((300 + RANDOM % 200))
    status="CRITICAL"
  fi

  temp_f=$(printf "%d.%02d" $((temp / 100)) $((temp % 100)))
  pressure_f=$(printf "%d.%02d" $((pressure / 100)) $((pressure % 100)))
  vibration_f=$(printf "%d.%02d" $((vibration / 100)) $((vibration % 100)))

  ts=$(date +%s)

  echo "{\"deviceId\":\"sensor-$((idx+1))\",\"timestamp\":${ts},\"temperature\":${temp_f},\"pressure\":${pressure_f},\"vibration\":${vibration_f},\"status\":\"${status}\"}"
}

# -------------------------------
# START
# -------------------------------
initDevices

msgNr=0

while true; do

  for ((i=0;i<DEVICE_COUNT;i++)); do

    payload=$(generateEvent $i)
    msgNr=$((msgNr + 1))

    echo "Publishing #$msgNr (sensor-$((i+1)))"

    ${PUBEXE} \
      -h ${HOST} \
      -p ${PORT} \
      -t ${TOPIC} \
      -q ${QOS} \
      -m "$payload" \
      -u ${USERNAME} \
      -P ${PASSWORD} \
      ${TLS}

    if [ $? -ne 0 ]; then
      echo "❌ Publish failed"
    fi

  done

  sleep 1
done