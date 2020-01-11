package com.mptyminds.dataflow;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;

import static com.mptyminds.dataflow.ExportUtil.prepareQuery;

public class Main {

    public static void main(String[] args) throws IOException {

        ExportPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).as(ExportPipelineOptions.class);

        if (null == pipelineOptions.getRunner()) {
            pipelineOptions.setRunner(DirectRunner.class);
        }

        final String partitionDate = pipelineOptions.getPartitionDate();
        final Long startTime = pipelineOptions.getStartTime();
        final Long endTime = pipelineOptions.getEndTime();

        // to partition directory date wise
        final String outputFilePrefix = String.format("dt=%s/%s", partitionDate, pipelineOptions.getFileNamePrefix());

        String query = prepareQuery(pipelineOptions, partitionDate, startTime, endTime);

        System.out.println("final Query formed: \n\n" + query);

        Pipeline pipeline = Pipeline.create(pipelineOptions);
        final Schema schema = ExportUtil.getAvroSchemaFromBQ(
                pipelineOptions.getProject(),
                pipelineOptions.getBigqueryDataset(),
                pipelineOptions.getBigqueryTableName());

        final PCollection<GenericRecord> genericRecordPCollection = pipeline.apply(
                BigQueryIO
                        .read(SchemaAndRecord::getRecord)
                        .fromQuery(query)
                        .withCoder(AvroCoder.of(schema)).usingStandardSql());

        genericRecordPCollection.apply(
                FileIO
                        .<GenericRecord>write()
                        .via(ParquetIO.sink(schema))
                        .to(pipelineOptions.getOutputFilePath())
                        .withNumShards(pipelineOptions.getOutputFileShardCount())
                        .withNaming((window, pane, numShards, shardIndex, compression) -> String.format(
                                "%s_%d_%d.%s",
                                outputFilePrefix,
                                startTime,
                                endTime,
                                "parq")));


        System.out.println("pipeline prepared.... running now.");
        pipeline.run();
    }


}
