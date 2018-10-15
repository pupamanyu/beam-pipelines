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

import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import javafx.util.Pair;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.io.IOException;

public class ValidateJsonDoFn extends DoFn<String, String> {

  public static final TupleTag<String> VALIDATEDJSON = new TupleTag<String>() {};
  public static final TupleTag<Pair<String, String>> INVALIDATEDJSON =
      new TupleTag<Pair<String, String>>() {};
  private final String jsonSchema;
  private final Counter validatedJson;
  private final Counter invalidatedJson;

  public ValidateJsonDoFn(String jsonSchema) {
    this.jsonSchema = jsonSchema;
    this.validatedJson = Metrics.counter(ValidateJsonDoFn.class, "validated-json-counts");
    this.invalidatedJson = Metrics.counter(ValidateJsonDoFn.class, "invalidated-json-counts");
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    try {
      ValidationUtils.validate(context.element(), this.jsonSchema);
      this.validatedJson.inc();
      context.output(VALIDATEDJSON, context.element());
    } catch (IOException | ProcessingException e) {
      Pair<String, String> errorData = new Pair<String, String>(context.element(), e.getMessage());
      this.invalidatedJson.inc();
      context.output(INVALIDATEDJSON, errorData);
    }
  }
}
