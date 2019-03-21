/*
#
# Copyright (C) 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
 */
package com.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class BQToBQ {

  private interface Options
      extends ApplicationNameOptions, PipelineOptions, DataflowPipelineOptions, BigQueryOptions {

    @Description("Input BigQuery Project Name")
    @Validation.Required
    ValueProvider<String> getInputProject();

    void setInputProject(ValueProvider<String> value);

    @Description("Input BigQuery Dataset Name")
    @Validation.Required
    ValueProvider<String> getInputDatasetName();

    void setInputDatasetName(ValueProvider<String> value);

    @Description("Input BigQuery Table Name")
    @Validation.Required
    ValueProvider<String> getInputTableName();

    void setInputTableName(ValueProvider<String> value);

    @Description("Output BigQuery Dataset Name")
    @Validation.Required
    ValueProvider<String> getOutputDatasetName();

    void setOutputDatasetName(ValueProvider<String> value);

    @Description("Output BigQuery Table Name")
    @Validation.Required
    ValueProvider<String> getOutputTableName();

    void setOutputTableName(ValueProvider<String> value);
  }

  private static final Logger LOG = LoggerFactory.getLogger(BQToBQ.class);

  public static void main(String[] args) {

    // Set Options from command line arguments
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    // Initialize Pipeline
    Pipeline p = Pipeline.create(options);

    // Set Output Table Parameters
    TableReference outputTableReference = new TableReference();
    outputTableReference.setProjectId(options.getProject());
    outputTableReference.setDatasetId(options.getOutputDatasetName().get());
    outputTableReference.setTableId(options.getOutputTableName().get());

    /* Set Schema for Output Table where output is stored */
    TableSchema outputTableSchema = new TableSchema();

    ArrayList<TableFieldSchema> fieldSchema = new ArrayList<TableFieldSchema>();

    /* Add Fields to the output table schema */

    fieldSchema.add(
        new TableFieldSchema().setName("rounded_trip_distance_miles").setMode("NULLABLE").setType("FLOAT"));

    fieldSchema.add(
        new TableFieldSchema()
            .setName("total_passengers")
            .setMode("NULLABLE")
            .setType("INTEGER"));

    outputTableSchema.setFields(fieldSchema);

    /* Set the SQL Query.
    This Query can be moved to the command line option, or an external SQL file on GCS, or as a message on Kafka */
    String aggregationQuery =
        "select round(trip_distance) as rounded_trip_distance_miles, sum(passenger_count) as total_passengers from `"
            + options.getInputProject()
            + "`."
            + options.getInputDatasetName()
            + "."
            + options.getInputTableName()
            + " group by rounded_trip_distance_miles";

    /* Configure the pipeline */
    p.apply(
            "Read from Input Table using SQL Query",
            BigQueryIO.readTableRows().fromQuery(aggregationQuery).usingStandardSql())
        .apply(
            "Write the Query Results to Output Table",
            BigQueryIO.writeTableRows()
                .withSchema(outputTableSchema)
                .to(outputTableReference)
                // This can be changed to WRITE_APPEND or WRITE_EMPTY
                /* Reference: https://beam.apache.org/releases/javadoc/2.10.0/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.Write.WriteDisposition.html#WRITE_TRUNCATE */
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    /* Run the pipeline */
    p.run().waitUntilFinish();
  }
}
