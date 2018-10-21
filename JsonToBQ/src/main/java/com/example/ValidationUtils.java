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

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import java.io.Serializable;
import org.json.JSONException;

public class ValidationUtils implements Serializable {

  private final String jsonSchema;

  public ValidationUtils(String jsonSchema) {
    this.jsonSchema = jsonSchema;
  }

  public  void validate(String json) throws ValidationException {
    try {
      Schema schema =
          SchemaLoader.builder()
              .schemaJson(new JSONObject(this.jsonSchema))
              .draftV7Support()
              .build()
              .load()
              .build();
      schema.validate(new JSONObject(json));
    } catch (JSONException jsex) {
      throw new ValidationException("JSON Parsing exception: " + jsex.getMessage());
    }
  }
}
