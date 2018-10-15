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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class JsonToTableRowDoFn extends DoFn<String, TableRow> {

  public static final TupleTag<TableRow> VALIDTABLEROWS = new TupleTag<TableRow>() {};
  public static final TupleTag<String> INVALIDTABLEROWS = new TupleTag<String>() {};
  private final Counter validTableRows;
  private final Counter invalidTableRows;

  public JsonToTableRowDoFn() {
    this.validTableRows = Metrics.counter(JsonToTableRowDoFn.class, "valid-tablerow-counts");
    this.invalidTableRows = Metrics.counter(JsonToTableRowDoFn.class, "invalid-tablerow-counts");
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    try {
      try (InputStream inputStream =
          new ByteArrayInputStream(context.element().getBytes(StandardCharsets.UTF_8))) {
        //noinspection deprecation
        context.output(TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER));
        this.validTableRows.inc();
      }
    } catch (IOException e) {
      this.invalidTableRows.inc();
    }
  }
}
