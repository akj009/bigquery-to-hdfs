package com.mptyminds.dataflow;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface ExportPipelineOptions extends SparkPipelineOptions, HadoopFileSystemOptions, BigQueryOptions {

    @Description("bigquery dataset to read schema from")
    String getBigqueryDataset();

    void setBigqueryDataset(String bigqueryDataset);

    @Description("bigquery table to read schema from")
    String getBigqueryTableName();

    void setBigqueryTableName(String bigqueryTableName);

    @Description("sql file path")
    String getSqlFilePath();

    void setSqlFilePath(String sqlFilePath);

    @Description("output file path")
    String getOutputFilePath();

    void setOutputFilePath(String outputFilePath);

    @Description("partition date to query from")
    @Default.String("2020-01-01")
    String getPartitionDate();

    void setPartitionDate(String partitionDate);

    @Description("start time to query from")
    @Default.Long(1577861994020L)
    Long getStartTime();

    void setStartTime(Long startTime);

    @Description("end time epoch query from")
    @Default.Long(1577861994853L)
    Long getEndTime();

    void setEndTime(Long endTime);

    @Description("file name prefix")
    String getFileNamePrefix();

    void setFileNamePrefix(String fileNamePrefix);

    @Description("output file shard count")
    @Default.Integer(1)
    Integer getOutputFileShardCount();

    void setOutputFileShardCount(Integer outputFileShardCount);

}
