#!/usr/bin/env bash

source ~alperrot/cluster/user-env.sh

set -xe

KMEANS_K=${K:-100}
KMEANS_INPUT=${KMEANS_INPUT:-/user/acalluaud/worldcitiespop.csv}
KMEANS_OUTPUT=${KMEANS_OUTPUT:-/user/acalluaud/result.csv}
KMEANS_COLUMNS=${KMEANS_COLUMNS:-5}

mvn compile
mvn package
yarn jar target/kmeans-0.1.0.jar $KMEANS_INPUT $KMEANS_OUTPUT $KMEANS_K $KMEANS_COLUMNS
