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

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ErrorDataToTableRowFn extends DoFn<KV<String, String>, TableRow> {
  private final Counter errors;
  private String partitionColumnName;
  private String partitionColumnValue;
  private TableRow errorTableRow;

  public ErrorDataToTableRowFn(
      ValueProvider<String> partitionColumnValue, ValueProvider<String> errorTablePartitionColumn) {
    this.partitionColumnValue = partitionColumnValue.get();
    this.partitionColumnName = errorTablePartitionColumn.get();
    this.errors = Metrics.counter(ErrorDataToTableRowFn.class, "error-counts");
  }

  @Setup
  public void doSetup() {
    this.errorTableRow = new TableRow();
  }

  private void insertErrorData(String inputRow, String errorMessage) {
    this.errorTableRow.set("input_row", inputRow);
    this.errorTableRow.set("error_message", errorMessage);
    this.errorTableRow.set(this.partitionColumnName, this.partitionColumnValue);
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    insertErrorData(context.element().getKey(), context.element().getValue());
    context.output(errorTableRow);
    this.errors.inc();
  }
}
