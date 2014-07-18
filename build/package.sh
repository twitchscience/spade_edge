#!/bin/bash --
set -e -u -o pipefail

export SRCDIR="/home/vagrant/go/src/github.com/TwitchScience/spade_edge"
export PKGDIR="/tmp/pkg"
export DEPLOYDIR="${PKGDIR}/deploy"
export SUPPORTDIR="${PKGDIR}/support"

mkdir -p ${DEPLOYDIR}/{bin,data}
cp -R ${SRCDIR}/spade_edge ${DEPLOYDIR}
cp ${SRCDIR}/build/scripts/* ${DEPLOYDIR}/bin
cp -R ${SRCDIR}/build/config ${DEPLOYDIR}
