#!/bin/bash

SPARK_APP_JAR="target/scala-2.12/demo_2.12-0.1.0-SNAPSHOT.jar"

SPARK_APP_CLASS="MonteCarloPageRankApp"

HDFS_INPUT_FILE="/tmp/wiki-Talk.txt"

OUTPUT_CSV="output/csv_files/MonteCarlo_performance_data.csv"

TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")

OUTPUT_DIR="output/csv_files/run_${TIMESTAMP}"

MEMORY_CONFIGS="1000m"

CONFIGS_CSV="${OUTPUT_DIR}/MC_configs.csv"

mkdir -p "$OUTPUT_DIR"

echo "Starting performance experiments..."
echo "Using JAR: $SPARK_APP_JAR"
echo "Using Class: $SPARK_APP_CLASS"
#echo "numWalkers,numSteps,resetProb" > $OUTPUT_CSV
#echo "memory_mb,runtime_sec" > $OUTPUT_CSV
echo "numWalkers,numSteps,resetProb" > $CONFIGS_CSV

for ((i=1; i<=5; i++)) do
  echo "-----------------------------------------------------"
  echo "RUNNING EXPERIMENT with Executor Memory: $MEMORY_CONFIGS"
  echo "-----------------------------------------------------"
  RANK_CSV="${OUTPUT_DIR}/MC_top_20_ranks_${i}_$MEMORY_CONFIGS.csv"
  echo "VertexID,Rank" > "$RANK_CSV"
  log_file="MonteCarlo_run_log_${i}_$MEMORY_CONFIGS.txt"

  spark-submit \
    --class $SPARK_APP_CLASS \
    --master spark://eduroam-141-23-203-101.wlan.tu-berlin.de:7077 \
    --name "Demo-Experiment-$MEMORY_CONFIGS" \
    --deploy-mode client \
    --driver-memory 1g \
    --num-executors 1 \
    --executor-cores 2 \
    --executor-memory $MEMORY_CONFIGS \
    --driver-java-options "--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED" \
    --conf spark.executor.extraJavaOptions="--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED" \
    $SPARK_APP_JAR \
    $HDFS_INPUT_FILE \
    2>&1 | tee $log_file

  if [ $? -ne 0 ]; then
      echo "ERROR: Spark job failed for memory config $MEMORY_CONFIGS. Check $log_file for details."
      continue
  fi

  runtime=$(grep "Total Application Runtime:" $log_file | awk -F': ' '{print $2}' | awk '{print $1}')

  mem_val=$(echo $MEMORY_CONFIGS | sed 's/[gGmM]//')

  vertexId=$(grep "Vertex ID: " $log_file | sed -E 's/.*Vertex ID: ([0-9]+).*PageRank: ([0-9]+),([0-9]+)/\1,\2.\3/')

  if [ -n "$runtime" ]; then
    echo "Found runtime: $runtime seconds."
    echo "$mem_val,$runtime" #>> $OUTPUT_CSV
    echo "$vertexId" >> $RANK_CSV
  else
    echo "WARNING: Could not find runtime in log file: $log_file"
  fi
done

num_walkers=$(grep "Number of walkers per node: " "$log_file" | awk -F': ' '{print $2}')
num_steps=$(grep "Number of steps: " "$log_file" | awk -F': ' '{print $2}')
reset_prob=$(grep "Reset probability: " "$log_file" | awk -F': ' '{print $2}')

echo "$num_walkers,$num_steps,$reset_prob" >> "$CONFIGS_CSV"
echo "-----------------------------------------------------"
echo "All experiments complete. Data saved to $OUTPUT_CSV"
#cat $OUTPUT_CSV