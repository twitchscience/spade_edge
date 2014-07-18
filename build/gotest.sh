#!/bin/bash --
set -e -o pipefail -u

export GOPATH="/home/vagrant/go"
export SRCDIR="${GOPATH}/src/github.com/TwitchScience/spade_edge"
export PATH=${PATH}:${GOPATH}/bin

cd ${SRCDIR}
godep go test -v ./...
