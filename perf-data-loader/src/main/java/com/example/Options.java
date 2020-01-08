/*
 * Copyright (c) 2020 Google Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.example;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface Options extends PipelineOptions, DataflowPipelineOptions {
  // Place Holder for extending to add any future pipeline options

  @Description("Source PubSub Subscription")
  @Validation.Required
  ValueProvider<String> getSubscription();

  void setSubscription(ValueProvider<String> value);

  @Description("Target BigQuery DataSet")
  @Validation.Required
  ValueProvider<String> getDataSet();

  void setDataSet(ValueProvider<String> value);

  @Description("Target BigQuery Table")
  @Validation.Required
  ValueProvider<String> getTable();

  void setTable(ValueProvider<String> value);

  @Description("Target BigQuery DataSet for Dead Letter")
  @Validation.Required
  ValueProvider<String> getDeadLetterDataSet();

  void setDeadLetterDataSet(ValueProvider<String> value);

  @Description("Target BigQuery Table for Dead Letter")
  @Validation.Required
  ValueProvider<String> getDeadLetterTable();

  void setDeadLetterTable(ValueProvider<String> value);
}
