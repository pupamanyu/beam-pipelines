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

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.everit.json.schema.ValidationException;

public class ValidateJsonDoFn extends DoFn<String, String> {

  public static final TupleTag<String> VALIDATEDJSON = new TupleTag<String>() {};
  public static final TupleTag<KV<String, String>> INVALIDATEDJSON =
      new TupleTag<KV<String, String>>() {};
  private final Counter validatedJson;
  private final Counter invalidatedJson;
  private final ValidationUtils validationUtils;

  public ValidateJsonDoFn(String jsonSchema) {
    this.validatedJson = Metrics.counter(ValidateJsonDoFn.class, "validated-json-counts");
    this.invalidatedJson = Metrics.counter(ValidateJsonDoFn.class, "invalidated-json-counts");
    this.validationUtils = new ValidationUtils(jsonSchema);
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    try {
      this.validationUtils.validate(context.element());
      this.validatedJson.inc();
      context.output(VALIDATEDJSON, context.element());
    } catch (ValidationException e) {
      KV<String, String> errorData = KV.of(context.element(), e.toJSON().toString());
      this.invalidatedJson.inc();
      context.output(INVALIDATEDJSON, errorData);
    }
  }
}
