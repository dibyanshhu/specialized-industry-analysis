#!/bin/bash

JOB_FILE=$1

docker exec -it spark-iceberg spark-submit \
  --master local[*] \
  /home/iceberg/spark-apps/${JOB_FILE}
