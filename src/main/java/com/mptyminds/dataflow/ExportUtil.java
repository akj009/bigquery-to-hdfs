package com.mptyminds.dataflow;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.LegacySQLTypeName;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ExportUtil {

    public static String prepareQuery(ExportPipelineOptions pipelineOptions, String partitionDate, Long startTime, Long endTime) throws IOException {
        String query = String.join("\t", Files.readAllLines(Paths.get(pipelineOptions.getSqlFilePath())));

        query = query.replace("%partition_time", partitionDate)
                .replace("%start_time", String.valueOf(startTime))
                .replace("%end_time", String.valueOf(endTime));
        return query;
    }

    public static Schema getAvroSchemaFromBQ(String projectId, String datasetName, String tableName) {
        BigQuery bigquery = BigQueryOptions.newBuilder()
                .setProjectId(projectId)
                .build()
                .getService();

        final com.google.cloud.bigquery.Schema schema = bigquery.getTable(datasetName, tableName).getDefinition().getSchema();

        final List<Field> avroFieldList = new ArrayList<>();

        if (schema != null) {
            schema.getFields().forEach(field -> {

                final String fieldName = field.getName();
                final LegacySQLTypeName fieldType = field.getType();

                final Schema.Type avroType = convertToAvroType(fieldType);

                SchemaBuilder.UnionAccumulator<Schema> schemaUnionAccumulator = SchemaBuilder.unionOf().type(avroType.getName());

                switch (avroType) {
                    case INT:
                        schemaUnionAccumulator = schemaUnionAccumulator.and().longType(); // big query int range can fall in long for avro
                        break;
                    case FLOAT:
                        schemaUnionAccumulator = schemaUnionAccumulator.and().doubleType(); // big query float range can fall in double for avro

                }

                final Field avroField = new Field(
                        fieldName, schemaUnionAccumulator.and().nullType().endUnion(), "doc", null
                );

                avroFieldList.add(avroField);
            });
        }

        return Schema.createRecord(tableName, "", datasetName, false, avroFieldList);
    }

    public static Schema.Type convertToAvroType(LegacySQLTypeName bigqueryType) {
        for (Schema.Type avroType : Schema.Type.values()) {
            if (bigqueryType.name().startsWith(avroType.name())) {
                return avroType;
            }
        }

        if (bigqueryType == LegacySQLTypeName.TIMESTAMP) {
            return Schema.Type.LONG;
        }

        return Schema.Type.STRING;
    }
}
