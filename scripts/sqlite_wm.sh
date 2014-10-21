#!/bin/bash

# Owl Platform World Model wrapper script
# Author: Robert Moore and the Owl Platform
PARAMS="$@"
BIN_FILE=/usr/local/bin/owl/sqlite3_world_model_server
LOG_FILE=/var/log/owl/world_model.log
SQLITE_FILE_PATH=/var/lib/owl/

UNBUFFER_BIN=`which unbuffer`
HAS_UNBUFFER=$?

cd $SQLITE_FILE_PATH

if ((0 == HAS_UNBUFFER)); then
  $UNBUFFER_BIN $BIN_FILE $PARAMS &>$LOG_FILE
else
  $BIN_FILE $PARAMS  &>$LOG_FILE
fi
