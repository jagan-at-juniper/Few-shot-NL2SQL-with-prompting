#!/bin/bash

job_name=${1:- }
day_of_task=${2:- 2020-09-10}
ENV=${3:- producction}
spark_zip=./wenfeng/spark_jobs_test.zip

if [ $job_name == "coverage_aggregator" ]; then
  job_class=analytics.jobs.stats_aggregator.StatsAggregator
  data_source="ap-events"
elif [ $job_name == "coverage_enricher" ]; then
  job_class="analytics.jobs.data_enrichment.DataEnrichment"
  data_source="ap_coverage_enrichment"
elif [ $job_name == "coverage_detection" ]; then
  job_class=analytics.jobs.event_aggregator.EventAggregator
  data_source="ap_coverage_detection"
else
  echo "Wrong parameters,  usage:"
  echo ""
  echo "--To update coverage_anomaly_stats from ap-event/sle-coverage-anomaly---"
  echo "    $ ap_coverage_submit.sh  coverage_aggregator 2020-09-10"
  echo "--To get ap_coverage_stats from enricher: 4 inputs data"
  echo "    $ ap_coverage_submit.sh  coverage_enricher 2020-09-10"
  echo "--To get generate events--"
  echo "    $ ap_coverage_submit.sh  coverage_detection 2020-09-10"
  echo ""
  return
fi

echo "job_name=$job_name job_class = $job_class, data_source=$data_source"


args=" --deploy-mode cluster --master yarn \
--name ${job_name} \
--py-files ${spark_zip} \
--files s3://mist-${ENV}-assets/enrichment_source/ap_mfg_info.csv \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.driver.memoryOverhead=1024m "

jobs=" s3://mist-production-assets/services/spark_jobs/runner-v0.5.45.py \
${job_class} ${day_of_task} ${day_of_task} \
--provider aws --env ${ENV}  --delay-spark-context  --data-source ${data_source}"

echo "jobs for day_of_task ${day_of_task} hour= ${day_of_task}"

for hr in {00..23};
 do
#   if [ ${hr} -lt 10 ]; then
#     hour=0$hr #$(printf "%02d" $hr)
#   else
#     hour=$hr
#   fi
   hour=$hr
   echo "-----start to job  $day_of_task_$hour -------$hour  $(date)  "
   AWS_CMD="spark-submit ${args} $jobs  --hour $hour"
   echo ${AWS_CMD}
#   ${AWS_CMD}
 done


