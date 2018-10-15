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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TransformJsonDoFn extends DoFn<String, String> {

  public static final TupleTag<String> XFORM_SUCCESS = new TupleTag<String>() {};
  public static final TupleTag<String> XFORM_FAILED = new TupleTag<String>() {};
  private static final Logger LOG = LoggerFactory.getLogger(TransformJsonDoFn.class);
  private final Counter xformSuccess;
  private final Counter jsonParseErrors;
  private final ObjectMapper objectMapper;
  private SimpleDateFormat DATE_FORMAT;
  private ValueProvider<String> timestampColumn;
  private ValueProvider<String> partitionColumn;
  private ValueProvider<String> dateFormat;
  private String timestampField;
  private String partitionField;

  public TransformJsonDoFn(
      ValueProvider<String> timestampColumn,
      ValueProvider<String> partitionColumn,
      ValueProvider<String> dateFormat) {
    this.jsonParseErrors = Metrics.counter(TransformJsonDoFn.class, "jsonparse-error-counts");
    this.xformSuccess = Metrics.counter(TransformJsonDoFn.class, "successful-transform-counts");
    this.timestampColumn = timestampColumn;
    this.partitionColumn = partitionColumn;
    this.dateFormat = dateFormat;
    this.objectMapper = new ObjectMapper();
  }

  private JsonNode insertTimestampPartition(JsonNode jsonNode) {
    if (jsonNode.has(this.timestampField)) {
      jsonNode =
          ((ObjectNode) jsonNode)
              .put(
                  this.partitionField,
                  DATE_FORMAT.format(new Date(jsonNode.get(this.timestampField).asLong())));
    } else {
      LOG.error(
          "The Input Data does not contain the Specified Timestamp Column {}. Load Job will fail",
          this.timestampColumn);
    }
    return jsonNode;
  }

  @StartBundle
  public void startBundle(StartBundleContext startBundleContext) {
    this.DATE_FORMAT = new SimpleDateFormat(dateFormat.get());
    this.timestampField = this.timestampColumn.get();
    this.partitionField = this.partitionColumn.get();
    this.DATE_FORMAT = new SimpleDateFormat(dateFormat.get());
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    try {
      JsonNode jsonNode = objectMapper.readTree(context.element());
      // Insert Timestamp Partition Column Field
      jsonNode = insertTimestampPartition(jsonNode);
      // Emit the Transformed Output
      context.output(XFORM_SUCCESS, jsonNode.toString());
      this.xformSuccess.inc();
    } catch (IOException e) {
      LOG.error("Unable to Parse JSON due to: {}", e.getMessage());
      this.jsonParseErrors.inc();
    }
  }
}
