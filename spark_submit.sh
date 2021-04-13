/usr/hdp/current/spark2-client/bin/spark-submit \
 --class com.prodapt.assignment.StreamAssignment \
 --master yarn \
 --deploy-mode cluster \
 --driver-memory 2g \
 --executor-memory 2g \
 --num-executors 2 \
 --executor-cores 2 \
--queue <queue-name> <jar-file-path> "<input file path>" "<output dir>"