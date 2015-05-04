#!/bin/bash --
set -e -u -o pipefail

cd -- "$(dirname -- "$0")"

eval "$(curl 169.254.169.254/latest/user-data/)"

KAFKA_CHROOT="/kafka/${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT}}"
KAFKA_BROKERS="$(/usr/bin/seeker --mode "kafka" --s3bucket "twitch-exhibitor" --exhibitorconfig "zk-exhibitor.conf" --kchroot ${KAFKA_CHROOT})" || KAFKA_BROKERS=""

export HOST="$(curl 169.254.169.254/latest/meta-data/hostname)"
export EDGE_VERSION="1"
export CROSS_DOMAIN_LOCATION="/opt/science/spade_edge/config/crossdomain.xml"
export STATSD_HOSTPORT="localhost:8125"
export GOMAXPROCS="4"
export CONFIG_PREFIX="s3://$S3_CONFIG_BUCKET/$VPC_SUBNET_TAG/$CLOUD_APP/$CLOUD_ENVIRONMENT"
aws s3 cp --region us-west-2 "$CONFIG_PREFIX/conf.sh" conf.sh
source conf.sh

if [ -z "${KAFKA_BROKERS}" ]
then
  echo "WARN: Could Not Talk to Zookeeper. Check your config."
  echo "WARN: Continuing without Kafka."
fi

# Optional config, often set in s3
# export MAX_LOG_LINES=1000000  # 1 million
# export MAX_LOG_AGE_SECS=600   # 10 minutes
# export MAX_AUDIT_LOG_LINES=1000000  # 1 million
# export MAX_AUDIT_LOG_AGE_SECS=600   # 10 minutes

exec ./spade_edge \
  -kafka_brokers "${KAFKA_BROKERS}" \
  -client_id "${HOST}" \
  -log_dir /mnt \
  -port ":80" \
  -stat_prefix "${CLOUD_APP}.${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT}}.${EC2_REGION}.${CLOUD_AUTO_SCALE_GROUP##*-}"
