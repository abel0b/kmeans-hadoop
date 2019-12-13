#!/usr/bin/env bash

set -xe

KMEANS_K=${K:-100}
KMEANS_INPUT=${KMEANS_INPUT:-/user/acalluaud/worldcitiespop.csv}
KMEANS_OUTPUT=${KMEANS_OUTPUT:-/user/acalluaud/result}
KMEANS_COLUMN=${KMEANS_COLUMN:-5}

mvn compile
mvn package
hdfs dfs -rm -r /user/acalluaud/result
yarn jar target/kmeans1D-0.1.0.jar $KMEANS_INPUT $KMEANS_OUTPUT $KMEANS_K $KMEANS_COLUMN
hdfs dfs -cat /user/acalluaud/result/part-r-00000

