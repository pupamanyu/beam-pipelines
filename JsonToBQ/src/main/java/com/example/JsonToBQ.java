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
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.UUID;

public class JsonToBQ {

  private static final Logger LOG = LoggerFactory.getLogger(JsonToBQ.class);
  public static Options options;

  public static void main(String[] args) {

    options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    options.setJobName(
        options.getJobTagPrefix().get()
            + '-'
            + options.getOutputTableName().get()
            + '-'
            + UUID.randomUUID().toString());

    Pipeline p = Pipeline.create(options);

    String bqSchema = SchemaUtils.getSchema(options.getBQSchemaFile().get());
    String jsonSchema = SchemaUtils.getSchema(options.getJSONSchemaFile().get());

    TableReference tableReference = new TableReference();
    tableReference.setProjectId(options.getProject());
    tableReference.setDatasetId(options.getOutputDatasetName().get());
    tableReference.setTableId(options.getOutputTableName().get());

    TableReference errorTableReference = new TableReference();
    errorTableReference.setProjectId(options.getProject());
    errorTableReference.setDatasetId(options.getOutputDatasetName().get());
    errorTableReference.setTableId(options.getErrorTableName().get());

    /* Set Schema for Error Table where invalidated input is stored */
    TableSchema errorTableSchema = new TableSchema();
    ArrayList<TableFieldSchema> fieldSchema = new ArrayList<TableFieldSchema>();
    fieldSchema.add(
        new TableFieldSchema()
            .setName(options.getErrorTablePartitionColumn().get())
            .setMode("NULLABLE")
            .setType("DATE"));
    fieldSchema.add(
        new TableFieldSchema().setName("input_row").setMode("NULLABLE").setType("STRING"));
    fieldSchema.add(
        new TableFieldSchema().setName("error_message").setMode("NULLABLE").setType("STRING"));
    errorTableSchema.setFields(fieldSchema);

    /* PCollectionList to hold Valid TableRows for bulk ingestion into Main Table*/
    PCollectionList<TableRow> tableRowPCollectionList = PCollectionList.empty(p);

    /* PCollectionList to hold Invalid Input for bulk ingestion into Error Table*/
    PCollectionList<TableRow> errorPCollectionList = PCollectionList.empty(p);

    for (String prefix : Splitter.on(",").trimResults().split(options.getInputPrefixes().get())) {
      ArrayList<String> prefixList =
          Lists.newArrayList((Splitter.on("-").trimResults().split(prefix)));
      /* TODO: Make these more generic prefix DATE, TIMESTAMP */
      prefixList.set(0, "year=" + prefixList.get(0));
      prefixList.set(1, "month=" + prefixList.get(1));
      prefixList.set(2, "day=" + prefixList.get(2));
      String subPrefix = Joiner.on('/').join(prefixList);
      String inputFilePattern =
          options.getInputDirectory().get()
              + '/'
              + subPrefix
              + '/'
              + "**"
              + options.getInputFilenameSuffix().get();
      /* Extract Lines from the files, Handle failed files separately. */
      PCollectionTuple inputRows =
          p.apply(
                  "Match Input Files for: " + prefix,
                  FileIO.match()
                      .filepattern(inputFilePattern)
                      .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
              .apply(
                  "Get Readable Files for: " + prefix,
                  FileIO.readMatches().withCompression(Compression.AUTO))
              .apply(
                  "Read File Line by Line for: " + prefix,
                  ParDo.of(new ReadableFileToStringDoFn())
                      .withOutputTags(
                          ReadableFileToStringDoFn.VALIDROWS,
                          TupleTagList.of(ReadableFileToStringDoFn.FAILEDFILES)));
      /* Desensitize the Rows by removing PII */
      PCollectionTuple desensitizedRows =
          inputRows
              .get(ReadableFileToStringDoFn.VALIDROWS)
              .apply(
                  "Desensitize Data for: " + prefix,
                  ParDo.of(
                          new DeSensitizeDoFn(
                              options.getSensitiveFields(), options.getGeoRootFieldName()))
                      .withOutputTags(
                          DeSensitizeDoFn.DESENSITIZED_SUCCESS,
                          TupleTagList.of(DeSensitizeDoFn.DESENSITIZED_FAILED)));

      /* Validate the JSON Rows, Handle invalid/mistyped JSON rows separately */
      PCollectionTuple jsonRows =
          desensitizedRows
              .get(DeSensitizeDoFn.DESENSITIZED_SUCCESS)
              .apply(
                  "Validate JSON Against the JSON Schema for: " + prefix,
                  ParDo.of(new ValidateJsonDoFn(jsonSchema))
                      .withOutputTags(
                          ValidateJsonDoFn.VALIDATEDJSON,
                          TupleTagList.of(ValidateJsonDoFn.INVALIDATEDJSON)));

      /* Success Path: Apply Transformations to JSON Rows, Handle failures separately */
      PCollectionTuple transformedJsonRows =
          jsonRows
              .get(ValidateJsonDoFn.VALIDATEDJSON)
              .apply(
                  "Transform/Mutate JSON Fields for: " + prefix,
                  ParDo.of(
                          new TransformJsonDoFn(
                              options.getTimestampColumn(),
                              options.getOutputTablePartitionColumn(),
                              options.getPartitionColumnDateFormat()))
                      .withOutputTags(
                          TransformJsonDoFn.XFORM_SUCCESS,
                          TupleTagList.of(TransformJsonDoFn.XFORM_FAILED)));

      /* Success Path: Mutate JSON to TableRow */
      PCollectionTuple inputTableRows =
          transformedJsonRows
              .get(TransformJsonDoFn.XFORM_SUCCESS)
              .apply(
                  "Mutate JSON to TableRow for: " + prefix,
                  ParDo.of(new JsonToTableRowDoFn())
                      .withOutputTags(
                          JsonToTableRowDoFn.VALIDTABLEROWS,
                          TupleTagList.of(JsonToTableRowDoFn.INVALIDTABLEROWS)));

      /* Success Path: Collect TableRow PCollection into a List for merging*/
      tableRowPCollectionList =
          tableRowPCollectionList.and(inputTableRows.get(JsonToTableRowDoFn.VALIDTABLEROWS));

      /* Filter invalid/mistyped JSON */
      PCollection<KV<String, String>> invalidatedJSONCollection =
          jsonRows.get(ValidateJsonDoFn.INVALIDATEDJSON);

      PCollection<TableRow> errorTableRows =
          invalidatedJSONCollection.apply(
              "Mutate Error Data Pair to TableRow for: " + prefix,
              ParDo.of(
                  new ErrorDataToTableRowFn(
                      ValueProvider.StaticValueProvider.of(prefix),
                      options.getErrorTablePartitionColumn())));

      /* Error Path: Collect Error Data PCollection into a List for merging */
      errorPCollectionList = errorPCollectionList.and(errorTableRows);
    }

    /* Merge Table Rows for valid Data */
    PCollection<TableRow> mergedValidTableRows =
        tableRowPCollectionList
            .apply(
                "Merge Validated TableRows for: " + options.getInputPrefixes().get(),
                Flatten.<TableRow>pCollections())
            .setCoder(TableRowJsonCoder.of());

    /* Merge TableRows for error Data */
    PCollection<TableRow> mergedErrorTableRows =
        errorPCollectionList
            .apply(
                "Merge Invalidated Error Data for: " + options.getInputPrefixes().get(),
                Flatten.<TableRow>pCollections())
            .setCoder(TableRowJsonCoder.of());

    /* Success Path: Load Validated TableRows to BQ */
    /* Load Valid Data into BQ */
    mergedValidTableRows.apply(
        "Load Validated Data into "
            + options.getOutputTableName()
            + " for: "
            + options.getInputPrefixes().get(),
        BigQueryIO.writeTableRows()
            .withSchema(SchemaUtils.getTableSchema(bqSchema))
            .to(tableReference)
            .withTimePartitioning(
                new TimePartitioning().setField(options.getOutputTablePartitionColumn().get()))
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    /* Error Path: Load Invalidated TableRows to BQ */
    /* Load Error Data into BQ */
    mergedErrorTableRows.apply(
        "Load Invalidated Data into "
            + options.getErrorTableName()
            + "  for: "
            + options.getInputPrefixes().get(),
        BigQueryIO.writeTableRows()
            .withSchema(errorTableSchema)
            .to(errorTableReference)
            .withTimePartitioning(
                new TimePartitioning().setField(options.getErrorTablePartitionColumn().get()))
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND));
    p.run();
  }
}
