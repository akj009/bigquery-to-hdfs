# BigqueryToHdfs Pipeline

This is a dataflow written in java to query data from bigquery and write them in parquet files in hdfs.
The job is intended to run on a in-house spark cluster using **Yarn** as resource manager.

## Getting Started

### Built with-

- Language: Java 8
- Build system: Maven
- ETL framework: Apache Beam
- Beam Version: 2.14
- Spark Version: <2.4.0

### How to build?

- The project can be built using maven with the following-

    `mvn clean install`
    
### How to test on local?

- This job can be tested on local using Beam's **DirectRunner**.
- If a local Spark Cluster is available, then master can be specified in 
the `spark-submit` command as well.

## Running on a Cluster

Currently, running on Google Cloud Dataflow using **DataflowRunner** and 
on Spark 2 Cluster using **SparkRunner** is supported in `pom.xml`

#### Running on Google Cloud Dataflow

1. Run the following command to submit the job-

```
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/google_application_credentials

java -jar /path/to/final-shaded.jar \
    --hdfsConfiguration=[{\"fs.default.name\":\"hdfs:/host:port\"}] \
    --project=<gcp-project> \
    --bigqueryDataset=my_dataset --bigqueryTableName=<my_table_name> \
    --tempLocation=gs://temp_bucket --outputFilePath=hdfs://host:port/user/test/ \
    --sqlFilePath=<path-to-sql> --runner=DataflowRunner
```
#### Running on Spark Cluster

1. Copy a suitable google application credentials file to all the nodes in the cluster.
The roles required as `BigQueryAdmin`, `StorageAdmin`.
2. Copy the jar file to any of the spark nodes.
3. Run the following commands to submit the job-

    ```
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/google_application_credentials
    
    spark2-submit --class com.mptyminds.dataflow.Main \
        --master yarn --deploy-mode client \
        --driver-memory 2g --executor-memory 1g --executor-cores 1 \
        --conf spark.yarn.appMasterEnv.GOOGLE_APPLICATION_CREDENTIALS=/path/to/google_application_credentials.json \
        --conf spark.yarn.executorEnv.GOOGLE_APPLICATION_CREDENTIALS=/path/to/google_application_credentials.json \
        --conf spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS=/path/to/google_application_credentials.json \
        /path/to/final-shaded.jar \
        --hdfsConfiguration=[{\"fs.default.name\":\"hdfs:/host:port\"}] \
        --sparkMaster=yarn --streaming=false \
        --project=<gcp-project> \
        --bigqueryDataset=my_dataset --bigqueryTableName=<my_table_name> \
        --tempLocation=gs://temp_bucket --outputFilePath=hdfs://host:port/user/test/ \
        --sqlFilePath=<path-to-sql> --runner=DataflowRunner
    ```