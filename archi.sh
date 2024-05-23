#!/bin/bash

# Get the current UTC time in the desired format
timestamp=$(date -u +"%Y-%m-%d %H:%M:%S")

LOG_FILE="$HOME/archivator/logs/$(date -u +"%Y-%m-%d_%H:%M:%S").log"

# Ensure the log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

# Function to log messages
log_message() {
  local MESSAGE="$1"
  local TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
  # Log to file
  echo "$TIMESTAMP : $MESSAGE" >> "$LOG_FILE"
  # Log to stdout
  echo $MESSAGE
}

# Run archivator for DEPENDANT tables and capture stdout and stderr
./bin/archi_linux_amd64 archive \
  --table=transactions --where="requestTime <= $timestamp" \
  --table=transactions --where="requestTime <= $timestamp" \
  --table=transactions --where="requestTime <= $timestamp" \
  2>&1 | while IFS= read -r line; do

  log_message "$line"
done

./bin/archi_linux_amd64 archive \
  --table=transactions --where="requestTime <= $timestamp" \
  --table=transactions --where="requestTime <= $timestamp" \
  --table=transactions --where="requestTime <= $timestamp" \
  2>&1 | while IFS= read -r line; do

  log_message "$line"
done

./bin/archi_linux_amd64 archive \
  --table=transactions --where="requestTime <= $timestamp" \
  --table=transactions --where="requestTime <= $timestamp" \
  --table=transactions --where="requestTime <= $timestamp" \
  2>&1 | while IFS= read -r line; do

  log_message "$line"
done
