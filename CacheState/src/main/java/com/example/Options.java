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

  @Description("BigTable Instance ID") @Validation.Required ValueProvider<String> getTableInstance();

  void setTableInstance(ValueProvider<String> value);

  @Description("BigTable Table Name") @Validation.Required ValueProvider<String> getTableName();

  void setTableName(ValueProvider<String> value);

  @Description("BigTable Column Family") @Validation.Required ValueProvider<String> getColumnFamily();

  void setColumnFamily(ValueProvider<String> value);

  @Description("GCS Bucket Name(For eg: gs://<bucket name>") @Validation.Required ValueProvider<String> getGcsBucket();

  void setGcsBucket(ValueProvider<String> value);

  @Description("Output File Name Prefix") @Validation.Required ValueProvider<String> getFileNamePrefix();

  void setFileNamePrefix(ValueProvider<String> value);

  @Description("Output File Number of Shards") @Validation.Required ValueProvider<Integer> getNumberOfShards();

  void setNumberOfShards(ValueProvider<Integer> value);
}
