#!/usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

function print_usage {
    echo "Install JStorm Package"
    echo "Uage: "
    echo " ./install_package.sh storm_version eg: ./install.sh 1.0.2-SNAPSHOT"
} 

if [ $# = 0 ]; then
   print_usage
   exit
fi

STORM_VERSION=$1
STORM_RELEASES=${bin}/storm-releases
STORM_HOME=${bin}/storm-current
STORM_CONF_DIR=${bin}/storm-conf
STORM_BIN_DIR=${STORM_HOME}/bin


if [ ! -x "${STORM_RELEASES}" ]; then  
   mkdir -p "${STORM_RELEASES}"  
fi

if [ ! -x ${STORM_RELEASES}/jstorm-${STORM_VERSION} ]; 
   then
      echo "Files exists!"
   else 
       rm -rf ${STORM_RELEASES}/jstorm-${STORM_VERSION}
fi 

mkdir -p ${STORM_RELEASES}/jstorm-${STORM_VERSION}
unzip  storm-${STORM_VERSION}.zip -d ${STORM_RELEASES}/

if [ -d ${STORM_HOME} ]; then 
   rm -rf  "${STORM_HOME}"
fi

ln -s ${STORM_RELEASES}/jstorm-${STORM_VERSION}  ${STORM_HOME}

if [ ! -x "$STORM_CONF_DIR" ]; then  
   mkdir -p "${STORM_CONF_DIR}"  
   cp -r ${STORM_HOME}/conf/* ${STORM_CONF_DIR}
   echo "!!! Please Modify ${STORM_CONF_DIR} " 
fi

ln -s ${bin}/storm-${STORM_VERSION}.zip ${STORM_HOME}/storm.zip

echo "export STORM_HOME=${STORM_HOME}" >> ${STORM_BIN_DIR}/storm-env.sh
echo "export STORM_CONF_DIR=${STORM_CONF_DIR} " >> ${STORM_BIN_DIR}/storm-env.sh
echo "export PATH=${STORM_BIN_DIR}:$PATH" >> ${STORM_BIN_DIR}/storm-env.sh
sh ${STORM_BIN_DIR}/storm-env.sh
