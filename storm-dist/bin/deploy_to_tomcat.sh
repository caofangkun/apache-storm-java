#!/usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

function print_usage {
    echo "Deploy JStorm UI War To Tomcat"
    echo "Uage: "
    echo " ./deploy_to_tomcat.sh"
}


if [ -n "${STORM_HOME}" ]; then
    RUNNER="${STORM_HOME}"
else
    if [ `command -v storm` ]; then
        RUNNER="storm"
    else
        echo "STORM_HOME is not set" >&2
        exit 1
    fi
fi

if [ $# = 0 ]; then
   print_usage
   exit
fi

