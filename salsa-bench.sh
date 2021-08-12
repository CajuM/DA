#!/bin/sh

shopt -s nullglob

ROOT=$(dirname $(dirname $(readlink -f $0)))
JAR=$(echo "$ROOT"/share/java/*.jar)
LOG4J=$(echo "$ROOT"/etc/log4j.properties)

function bench() {
	executors=$1
	cores=$2
	ram=$3

	dataset=$4
	alg=$5
	iters=$6

	spark-submit \
		--master yarn \
		--deploy-mode cluster \
		--num-executors $executors \
		--executor-cores $cores \
		--executor-memory $ram \
		--files "$LOG4J" \
		"$JAR" \
			"$dataset" \
			"$alg" \
			"$iters"

	while :; do
		out="$(yarn application -list -appStates 'NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING' 2>/dev/null | grep -v '\[main\]' | tail -n+3)"
		[ -z "$out" ] && break
	done
}

dataset=$1
cores_per_executor=$2
ram_per_executor=$3

hdfs dfs -cp file://$(readlink -f $dataset) $(basename $dataset)

for executors in $(seq 0 8); do
	if [ $executors -eq 0 ]; then
		executors=1
		cores=1
	else
		cores=$cores_per_executor
	fi

	for alg in "mySALSA1" "mySALSA2" "mySALSA3"; do
		for iter in 1 4 8 16 32; do
			bench $executors $cores $ram_per_executor $dataset $alg $iter
		done
	done
done
