#!/bin/bash --
set -e -u -o pipefail

cd -- "$(dirname -- "$0")"

eval "$(curl 169.254.169.254/latest/user-data/)"

export HOST="$(curl 169.254.169.254/latest/meta-data/hostname)"
export EDGE_VERSION="2"
export CROSS_DOMAIN_LOCATION="/opt/science/spade_edge/config/crossdomain.xml"
export STATSD_HOSTPORT="localhost:8125"
export GOMAXPROCS="4"
export CONFIG_PREFIX="s3://$S3_CONFIG_BUCKET/$VPC_SUBNET_TAG/$CLOUD_APP/$CLOUD_ENVIRONMENT"
CORS_ORIGINS=""  # Often overridden in conf.sh
aws s3 cp --region us-west-2 "$CONFIG_PREFIX/conf.sh" conf.sh
source conf.sh

# Optional config, often set in conf.sh
# export MAX_LOG_LINES=1000000  # 1 million
# export MAX_LOG_AGE_SECS=600   # 10 minutes
# export MAX_AUDIT_LOG_LINES=1000000  # 1 million
# export MAX_AUDIT_LOG_AGE_SECS=600   # 10 minutes

exec ./spade_edge \
  -event_log_name="spade-edge-${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT:-$USER-dev}}" \
  -audit_log_name="spade-audits-${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT:-$USER-dev}}" \
  -event_error_name="uploader-error-spade-edge-${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT:-$USER-dev}}" \
  -kinesis_stream_name="spade-edge-${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT:-$USER-dev}}" \
  -log_dir /mnt \
  -port ":80" \
  -cors_origins "${CORS_ORIGINS}" \
  -stat_prefix "${CLOUD_APP}.${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT}}.${EC2_REGION}.${CLOUD_AUTO_SCALE_GROUP##*-}"
