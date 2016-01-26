#!/bin/bash -ex

pushd `dirname $0` > /dev/null
base=$(pwd -P)
popd > /dev/null

# Gather some data about the repo
source $base/vars.sh

# Send a null Job status check
[ `curl -s -o /dev/null -w "%{http_code}" http://pz-ingest.cf.piazzageo.io` = 200 ]
