#!/bin/bash

SPARK_APP_JAR="target/scala-2.12/demo_2.12-0.1.0-SNAPSHOT.jar"

SPARK_APP_CLASS="SnapPageRankApp"

HDFS_INPUT_FILE="/tmp/wiki-Talk.txt"

OUTPUT_CSV="GraphX_performance_data.csv"

MEMORY_CONFIGS="400m 500m 700m 1g 1300m 1500m 1700m 2g"

echo "Starting performance experiments..."
echo "Using JAR: $SPARK_APP_JAR"
echo "Using Class: $SPARK_APP_CLASS"
echo "memory_mb,runtime_sec" > $OUTPUT_CSV

for mem in $MEMORY_CONFIGS; do
  echo "-----------------------------------------------------"
  echo "RUNNING EXPERIMENT with Executor Memory: $mem"
  echo "-----------------------------------------------------"
  log_file="run_log_${mem}.txt"

  spark-submit \
    --class $SPARK_APP_CLASS \
    --master spark://ninamac.fritz.box:7077 \
    --name "Demo-Experiment-$mem" \
    --deploy-mode client \
    --driver-memory 1g \
    --num-executors 1 \
    --executor-cores 2 \
    --executor-memory $mem \
    --driver-java-options "--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED" \
    --conf spark.executor.extraJavaOptions="--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED" \
    $SPARK_APP_JAR \
    $HDFS_INPUT_FILE \
    2>&1 | tee $log_file

  if [ $? -ne 0 ]; then
      echo "ERROR: Spark job failed for memory config $mem. Check $log_file for details."
      continue
  fi

  runtime=$(grep "Total Application Runtime:" $log_file | awk -F': ' '{print $2}' | awk '{print $1}')

  mem_val=$(echo $mem | sed 's/[gGmM]//')

  if [ -n "$runtime" ]; then
    echo "Found runtime: $runtime seconds."
    echo "$mem_val,$runtime" >> $OUTPUT_CSV
  else
    echo "WARNING: Could not find runtime in log file: $log_file"
  fi
done

echo "-----------------------------------------------------"
echo "All experiments complete. Data saved to $OUTPUT_CSV"
cat $OUTPUT_CSV