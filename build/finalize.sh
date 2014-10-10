#!/bin/bash --
set -e -u -o pipefail

BASEDIR="/opt/science"
EDGEDIR="${BASEDIR}/spade_edge"
CONFDIR="${EDGEDIR}/config"

UPSTARTDIR="/etc/init"
PKGDIR="/tmp/pkg"

mv ${PKGDIR}/deploy ${EDGEDIR}
chmod +x ${EDGEDIR}/bin/*

sudo apt-get update 
sudo apt-get install -y dpkg

curl -O http://science.twitch.tv/debs/kafka_finder_1.0.3_amd64.deb
sudo dpkg -i kafka_finder_1.0.3_amd64.deb

# Setup upstart
mv ${CONFDIR}/mount_ebslvm.conf ${UPSTARTDIR}/mount_ebslvm.conf
mv ${CONFDIR}/upstart.conf ${UPSTARTDIR}/spade_edge.conf
