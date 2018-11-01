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
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import org.apache.beam.sdk.io.FileSystems;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.google.cloud.bigquery.StandardSQLTypeName.valueOf;

public class SchemaUtils {

  private static Gson getGson() {
    JsonDeserializer<StandardSQLTypeName> typeDeserializer
            = (jsonElement, type, deserializationContext) -> valueOf(jsonElement.getAsString());

    JsonDeserializer<FieldList> subFieldsDeserializer
            = (jsonElement, type, deserializationContext) -> {
              Field[] fields
              = deserializationContext.deserialize(jsonElement.getAsJsonArray(), Field[].class);
              return FieldList.of(fields);
            };

    return new GsonBuilder()
            .registerTypeAdapter(StandardSQLTypeName.class, typeDeserializer)
            .registerTypeAdapter(FieldList.class, subFieldsDeserializer)
            .create();
  }

  static TableSchema getTableSchema(String schema) {
    TableSchema tableSchema = new TableSchema();
    TableFieldSchema[] fields = getGson().fromJson(schema, TableFieldSchema[].class);
    tableSchema.setFields(Arrays.asList(fields));
    return tableSchema;
  }

  static String getSchema(String schemaFile) {
    String jsonSchema = null;
    try {
      ReadableByteChannel chan = FileSystems.open(FileSystems.matchNewResource(schemaFile, false));
      try (InputStream stream = Channels.newInputStream(chan)) {
        jsonSchema = CharStreams.toString(new InputStreamReader(stream, StandardCharsets.UTF_8));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return jsonSchema;
  }
}
