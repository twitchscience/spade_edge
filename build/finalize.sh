#!/bin/bash --
set -e -u -o pipefail

BASEDIR="/opt/science"
EDGEDIR="${BASEDIR}/spade_edge"
CONFDIR="${EDGEDIR}/config"

UPSTARTDIR="/etc/init"
PKGDIR="/tmp/pkg"

mv ${PKGDIR}/deploy ${EDGEDIR}
chmod +x ${EDGEDIR}/bin/*

# Setup upstart
mv ${CONFDIR}/mount_ebslvm.conf ${UPSTARTDIR}/mount_ebslvm.conf
mv ${CONFDIR}/upstart.conf ${UPSTARTDIR}/spade_edge.conf
