/*
 * Copyright 2022 Google.
 * This software is provided as-is, without warranty or representation for any use or purpose.
 * Your use of it is subject to your agreement with Google.
 */

package com.example.config;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface FakeRepartitionOptions extends PipelineOptions, DataflowPipelineOptions {
  @Description("Number of Data Partitions")
  @Validation.Required
  ValueProvider<Integer> getNumPartitions();

  void setNumPartitions(ValueProvider<Integer> value);

  @Description("Total Number of Fake Rows to be inserted")
  @Validation.Required
  ValueProvider<Long> getNumFakeRows();

  void setNumFakeRows(ValueProvider<Long> value);

  @Description("BigQuery DataSet")
  @Validation.Required
  ValueProvider<String> getDataset();

  void setDataset(ValueProvider<String> value);

  @Description("BigQuery Table")
  @Validation.Required
  ValueProvider<String> getTable();

  void setTable(ValueProvider<String> value);
}
