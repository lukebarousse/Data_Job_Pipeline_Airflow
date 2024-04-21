# Dataproc Python Files

Note: These files are included for example purposes only and may or may not match the current versions of the files in the Dataproc cluster.

---
### Spark Cluster Creation

```
REGION=us-central1
ZONE=us-central1-a
CLUSTER_NAME=spark-cluster
BUCKET_NAME=dataproc-cluster-gsearch

gcloud dataproc clusters create ${CLUSTER_NAME} \
 --enable-component-gateway \
 --region ${REGION} \
 --zone ${ZONE} \
 --bucket ${BUCKET_NAME} \
 --master-machine-type n2-standard-2 \
 --master-boot-disk-size 500 \
 --num-workers 2 \
 --worker-machine-type n2-standard-2 \
 --worker-boot-disk-size 500 \
 --image-version 1.5-debian10 \
 --optional-components ANACONDA,JUPYTER \
 --project job-listings-366015 \
 --properties=^#^spark:spark.jars.packages=com.johnsnowlabs.nlp:spark-nlp_2.12:4.2.6,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1 \
 --metadata 'PIP_PACKAGES=spark-nlp spark-nlp-display' \
 --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh
```