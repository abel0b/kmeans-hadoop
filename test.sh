#!/usr/bin/env bash

source ~alperrot/cluster/user-env.sh

set -xe

KMEANS_K=${K:-100}
KMEANS_INPUT=${KMEANS_INPUT:-/user/acalluaud/worldcitiespop.csv}
KMEANS_OUTPUT=${KMEANS_OUTPUT:-/user/acalluaud/result}
KMEANS_COLUMN=${KMEANS_COLUMN:-5}

mvn compile
mvn package
hdfs dfs -rm -r -f $KMEANS_OUTPUT
yarn jar target/kmeans1D-0.1.0.jar $KMEANS_INPUT $KMEANS_OUTPUT $KMEANS_K $KMEANS_COLUMN
hdfs dfs -cat $KMEANS_OUTPUT/part-r-00000

