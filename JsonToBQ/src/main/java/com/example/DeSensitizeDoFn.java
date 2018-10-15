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
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

public class DeSensitizeDoFn extends DoFn<String, String> {

  public static final TupleTag<String> DESENSITIZED_SUCCESS = new TupleTag<String>() {};
  public static final TupleTag<String> DESENSITIZED_FAILED = new TupleTag<String>() {};

  private static final Logger LOG = LoggerFactory.getLogger(DeSensitizeDoFn.class);
  // Fuzz Distance in Meters for Geo Fuzzing
  private static final int FUZZ_DISTANCE = 5000;
  private static EncryptUtils encryptUtils;
  private final Counter deSensitizeCounts;
  private final ObjectMapper objectMapper;
  private ArrayList<String> sensitiveFieldsList;
  private ValueProvider<String> sensitiveFields;
  private ValueProvider<String> geoFieldName;
  private String geoField;

  public DeSensitizeDoFn(
      ValueProvider<String> sensitiveFields, ValueProvider<String> geoFieldName) {
    this.deSensitizeCounts = Metrics.counter(DeSensitizeDoFn.class, "desensitized-row-counts");
    this.sensitiveFields = sensitiveFields;
    this.geoFieldName = geoFieldName;
    this.objectMapper = new ObjectMapper();
  }

  private JsonNode removeField(JsonNode jsonNode, String field) {
    return ((ObjectNode) jsonNode).remove(field);
  }

  private JsonNode deSensitizeField(JsonNode jsonNode, String sensitiveField) {
    if (jsonNode.hasNonNull(sensitiveField)) {
      jsonNode =
          ((ObjectNode) jsonNode)
              .put(
                  sensitiveField,
                  encryptUtils.deSensitize(jsonNode.get(sensitiveField).toString()));
    }
    return jsonNode;
  }

  private JsonNode deSensitizeFields(JsonNode jsonNode) {
    for (String sensitiveField : this.sensitiveFieldsList)
      jsonNode = deSensitizeField(jsonNode, sensitiveField);
    return jsonNode;
  }

  private JsonNode fuzzGeoFields(JsonNode jsonNode) {
    if (jsonNode.hasNonNull(this.geoField)) {
      JsonNode geoJsonNode = jsonNode.get(this.geoField);
      if (geoJsonNode.hasNonNull("lat") && geoJsonNode.hasNonNull("long")) {
        double latitude = geoJsonNode.get("lat").asDouble();
        double longitude = geoJsonNode.get("long").asDouble();
        jsonNode =
            ((ObjectNode) jsonNode)
                .set(
                    this.geoField,
                    ((ObjectNode) geoJsonNode)
                        .put("lat", LatLonFuzzer.fuzzLatitude(latitude, FUZZ_DISTANCE))
                        .put(
                            "long",
                            LatLonFuzzer.fuzzLongitude(latitude, longitude, FUZZ_DISTANCE)));
      }
    }
    return jsonNode;
  }

  @StartBundle
  public void startBundle(StartBundleContext startBundleContext) {
    this.sensitiveFieldsList =
        Lists.newArrayList(Splitter.on(',').trimResults().split(this.sensitiveFields.get()));
    this.geoField = geoFieldName.get();
    encryptUtils = new EncryptUtils();
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    try {
      JsonNode jsonNode = objectMapper.readTree(context.element());
      // Fuzz the Geo Fields for Compliance reasons
      jsonNode = fuzzGeoFields(jsonNode);
      // Encrypt the Sensitive PII fields for Compliance Reasons
      jsonNode = deSensitizeFields(jsonNode);
      // Emit the Desensitized Output
      context.output(DESENSITIZED_SUCCESS, jsonNode.toString());
      this.deSensitizeCounts.inc();
    } catch (IOException e) {
      LOG.error("Failed to Desensitize Data due to: {}", e.getMessage());
    }
  }
}
