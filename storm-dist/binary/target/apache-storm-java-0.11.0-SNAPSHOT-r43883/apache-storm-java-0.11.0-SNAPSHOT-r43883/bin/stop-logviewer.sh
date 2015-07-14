#!/usr/bin/env bash

usage="Usage: stop-logviewer.sh"

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

. "$bin"/storm-config.sh

"$STORM_HOME"/bin/storm-daemon.sh --config $STORM_CONF_DIR --script "$bin"/storm stop logviewer

