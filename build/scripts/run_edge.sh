#!/bin/bash --
set -e -u -o pipefail

cd -- "$(dirname -- "$0")"

eval "$(curl 169.254.169.254/latest/user-data/)"

export HOST="$(curl 169.254.169.254/latest/meta-data/hostname)"
export EDGE_VERSION="1"
export CROSS_DOMAIN_LOCATION="/opt/science/spade_edge/config/crossdomain.xml"
export STATSD_HOSTPORT="localhost:8125"

export GOMAXPROCS="3"
exec ../spade_edge \
  -kafka_brokers "${KAFKA_BROKERS} \
  -log_dir /mnt \
  -port ":80" \
  -stat_prefix "${CLOUD_APP}.${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT}}.${EC2_REGION}.${CLOUD_AUTO_SCALE_GROUP##*-}"
