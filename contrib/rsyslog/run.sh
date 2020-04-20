#!/bin/bash

export KAFKA_BROKER=localhost:9092 
exec /usr/sbin/rsyslogd -n -iNONE -f ./rsyslog.conf

