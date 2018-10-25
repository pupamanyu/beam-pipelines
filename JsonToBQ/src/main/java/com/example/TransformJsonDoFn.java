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
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

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
  private final Boolean sanitizeJson = JsonToBQ.options.getSanitizeJson().get();
  private final Boolean stringifyCustomData = JsonToBQ.options.getStringifyCustomData().get();
  private final String customDataField = JsonToBQ.options.getCustomDataField().get();

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

  static String sanitizeString(String str) {
    return str.replace(": ", "_").replace(".", "_").replace(" ", "_");
  }

  static JsonNode sanitizePropertyNames(JsonNode json) {
    Map<String, JsonNode> toBeSanitized = new HashMap<>();
    Map<String, JsonNode> objects = new HashMap<>();
    Iterator<Map.Entry<String, JsonNode>> iter = json.fields();
    while (iter.hasNext()) {
      Map.Entry<String, JsonNode> elt = iter.next();
      if (elt.getValue().isObject()) {
        JsonNode sanitized = sanitizePropertyNames(elt.getValue());
        objects.put(elt.getKey(), sanitized);
      } else if (!elt.getValue().isArray()){
        if (elt.getKey().contains(": ") || elt.getKey().contains(".") || elt.getKey().contains(" ")){
          toBeSanitized.put(elt.getKey(), null);
        }
      }
    }
    ObjectNode objNode = (ObjectNode) json;
    for (String key : toBeSanitized.keySet()) {
      objNode.set(sanitizeString(key), toBeSanitized.get(key) == null ? objNode.get(key) : toBeSanitized.get(key));
      objNode.remove(key);
    }
    for (String key : objects.keySet()) {
      objNode.set(sanitizeString(key), toBeSanitized.get(key) == null ? objNode.get(key) : toBeSanitized.get(key));
    }
    return objNode;
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

  static JsonNode stringifyCustomData(JsonNode json, String customPropertyName) {
    ObjectNode objNode = (ObjectNode) json;
    if (objNode.has(customPropertyName)) {
      objNode.put(customPropertyName, objNode.get(customPropertyName).toString());
    }
    return objNode;
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
      if (sanitizeJson) {
        jsonNode = sanitizePropertyNames(jsonNode);
      }
      if (stringifyCustomData) {
        jsonNode = stringifyCustomData(jsonNode, this.customDataField);
      }
      // Emit the Transformed Output
      context.output(XFORM_SUCCESS, jsonNode.toString());
      this.xformSuccess.inc();
    } catch (IOException e) {
      LOG.error("Unable to Parse JSON due to: {}", e.getMessage());
      this.jsonParseErrors.inc();
    }
  }
}
