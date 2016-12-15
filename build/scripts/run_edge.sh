#!/bin/bash --
set -e -u -o pipefail

cd -- "$(dirname -- "$0")"

for line in $( cat /etc/environment ) ; do export $(echo $line | tr -d '"') ; done
export HOST="$(curl 169.254.169.254/latest/meta-data/hostname)"
export EDGE_VERSION="2"
export CROSS_DOMAIN_LOCATION="/opt/science/spade_edge/config/crossdomain.xml"
export GOMAXPROCS="4"
export CONFIG_PREFIX="s3://$S3_CONFIG_BUCKET/$VPC_SUBNET_TAG/$CLOUD_APP/${CLOUD_ENVIRONMENT}"
export AWS_REGION=us-west-2
export AWS_DEFAULT_REGION=$AWS_REGION # aws-cli uses AWS_DEFAULT_REGION, aws-sdk-go uses AWS_REGION
aws s3 cp "$CONFIG_PREFIX/conf.sh" conf.sh
aws s3 cp "$CONFIG_PREFIX/conf.json" conf.json
source conf.sh

exec ./spade_edge -config conf.json \
                  -stat_prefix "${OWNER}.${CLOUD_APP}.${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT}}.${EC2_REGION}.${CLOUD_AUTO_SCALE_GROUP##*-}"
