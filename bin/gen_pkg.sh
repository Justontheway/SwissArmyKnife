#!/bin/bash

BASE_DIR=..

cd ${BASE_DIR}
sbt clean package publish-local
cd -
