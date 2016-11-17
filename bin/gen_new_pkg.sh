#!/bin/bash

ORIG_PKG=com.rrx.jdb.util
NEW_PKG=swiss.army.knife

ARC_MAIN_DIR=src.main.scala
ARC_TEST_DIR=src.test.scala

ORIG_DIR=..
NEW_DIR=../${1-util_pkg}

run() {
    echo "$@"
    $@
}

RUN=run

${RUN} rm -rf ${NEW_DIR}
${RUN} mkdir -p ${NEW_DIR}
${RUN} mkdir -p ${NEW_DIR}/bin
${RUN} mkdir -p ${NEW_DIR}/$(echo ${ARC_MAIN_DIR} | sed 's/\./\//g')
${RUN} mkdir -p ${NEW_DIR}/$(echo ${ARC_TEST_DIR} | sed 's/\./\//g')
${RUN} mkdir -p ${NEW_DIR}/$(echo ${ARC_MAIN_DIR}.${NEW_PKG} | sed 's/\./\//g')
${RUN} mkdir -p ${NEW_DIR}/$(echo ${ARC_TEST_DIR}.${NEW_PKG} | sed 's/\./\//g')

${RUN} cp ${ORIG_DIR}/bin/* ${NEW_DIR}/bin -rf
${RUN} cp ${ORIG_DIR}/build.sbt ${NEW_DIR}/build.sbt
${RUN} cp ${ORIG_DIR}/$(echo ${ARC_MAIN_DIR}.${ORIG_PKG}/* | sed 's/\./\//g') ${NEW_DIR}/$(echo ${ARC_MAIN_DIR}.${NEW_PKG} | sed 's/\./\//g') -rf
${RUN} cp ${ORIG_DIR}/$(echo ${ARC_TEST_DIR}.${ORIG_PKG}/* | sed 's/\./\//g') ${NEW_DIR}/$(echo ${ARC_TEST_DIR}.${NEW_PKG} | sed 's/\./\//g') -rf

SED_FROM=$(echo ${ORIG_PKG} | sed 's/\./\//g')
SED_TO=$(echo ${NEW_PKG} | sed 's/\./\//g')
SED_FROM=$(echo ${ORIG_PKG} | sed 's/\./\\\./g')
SED_TO=$(echo ${NEW_PKG} | sed 's/\./\\\./g')
SED_FROM=${ORIG_PKG}
SED_TO=${NEW_PKG}
find ${NEW_DIR}/src -type f | xargs sed -i "s/${SED_FROM}/${SED_TO}/g"

