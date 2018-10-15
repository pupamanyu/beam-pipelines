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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingMessage;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;

import java.io.IOException;

public class ValidationUtils {
  public static final String JSON_SCHEMA_IDENTIFIER = "http://json-schema.org/draft-04/schema";
  public static final String JSON_SCHEMA_IDENTIFIER_ELEMENT = "$schema";

  private static void validate(JsonNode jsonNode, JsonSchema jsonSchema)
      throws ProcessingException {
    ProcessingReport processingReport = jsonSchema.validate(jsonNode);
    if (!processingReport.isSuccess()) {
      for (ProcessingMessage processingMessage : processingReport) {
        throw new ProcessingException(processingMessage);
      }
    }
  }

  public static void validate(String json, String jsonSchema)
      throws IOException, ProcessingException {
    validate(getJson(json), getJsonSchema(jsonSchema));
  }

  private static JsonNode getJson(String json) throws IOException {
    return JsonLoader.fromString(json);
  }

  private static JsonSchema getJsonSchema(String jsonSchema)
      throws ProcessingException, IOException {
    JsonNode jsonNode = getJson(jsonSchema);
    final JsonNode schemaIdentifier = jsonNode.get(JSON_SCHEMA_IDENTIFIER_ELEMENT);
    if (schemaIdentifier.isNull()) {
      ((ObjectNode) jsonNode).put(JSON_SCHEMA_IDENTIFIER_ELEMENT, JSON_SCHEMA_IDENTIFIER);
    }
    final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
    return factory.getJsonSchema(getJson(jsonSchema));
  }
}
