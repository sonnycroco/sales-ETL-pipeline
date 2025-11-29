
#!/bin/bash


# This script requires one argument: a date in the format YYYY-MM-DD.
# If no argument is provided, it prints a usage message and exits.
# The provided date is stored in RUN_DATE and passed to a Spark job.
# The script then executes spark-submit inside the spark-master
# Docker container to run the sales_etl_job.py Spark application with the given date.

# Check for argument
if [ -z "$1" ]; then
  echo " Usage: ./run_sales.sh <YYYY-MM-DD>"
  exit 1
fi

RUN_DATE="$1"

echo "Triggering sales_etl.py for $RUN_DATE..."

# Run spark-submit inside spark-master container
docker exec spark-master spark-submit /opt/spark-apps/sales_etl/scripts/sales_etl_job.py "$RUN_DATE"

echo "Job completed for $RUN_DATE"
