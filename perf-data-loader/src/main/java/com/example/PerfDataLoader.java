/*
 * Copyright (c) 2020 Google Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.example;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class PerfDataLoader {

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    TupleTag<TableRow> MAIN_OUT = new TupleTag<TableRow>();

    TupleTag<TableRow> DEADLETTER_OUT = new TupleTag<TableRow>();
    TableReference tableSpec =
        new TableReference()
            .setProjectId(options.getProject())
            .setDatasetId(options.getDataSet().get())
            .setTableId(options.getTable().get());

    Pipeline p = Pipeline.create(options);

    ParDo.MultiOutput<String, TableRow> processMetrics =
        ParDo.of(new MetricDoFn(MAIN_OUT, DEADLETTER_OUT))
            .withOutputTags(MAIN_OUT, TupleTagList.of(DEADLETTER_OUT));

    PCollectionTuple pCollectionTuple =
        p.apply(
                "Read Metrics from PubSub",
                PubsubIO.readStrings().fromSubscription(options.getSubscription()))
            .apply("Process Metrics Data", processMetrics);

    PCollection<TableRow> tableRowPCollection =
        pCollectionTuple.get(MAIN_OUT).setCoder(TableRowJsonCoder.of());

    // Happy Path

    WriteResult writeResult =
        tableRowPCollection.apply(
            "Insert Metrics Data into BQ",
            BigQueryIO.writeTableRows()
                .to(tableSpec)
                .withExtendedErrorInfo()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

    // Dead Path
    // Filter and Process Insert Errors / Malformed(non-confirming) Metric Data into Dead Letter
    // Table(Error Table)
    TableReference errorTableSpec =
        new TableReference()
            .setProjectId(options.getProject())
            .setDatasetId(options.getDeadLetterDataSet().get())
            .setTableId(options.getDeadLetterTable().get());

    PCollection<TableRow> insertErrorPCollection =
        writeResult
            .getFailedInsertsWithErr()
            .apply("Process Insert Errors", ParDo.of(new ProcessInsertErrorDoFn()));

    PCollection<TableRow> deadLetterPCollection =
        pCollectionTuple.get(DEADLETTER_OUT).setCoder(TableRowJsonCoder.of());
    PCollectionList<TableRow> errorPCollection =
        PCollectionList.of(deadLetterPCollection).and(insertErrorPCollection);
    errorPCollection
        .apply("Merge error data", Flatten.pCollections())
        .apply(
            "Insert Errors into Dead Letter Table",
            BigQueryIO.writeTableRows()
                .to(errorTableSpec)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));
    p.run();
  }
}
