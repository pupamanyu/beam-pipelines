package com.example;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface Options extends PipelineOptions, DataflowPipelineOptions {
  @Description("Random Key Length")
  @Default.Integer(16)
  ValueProvider<Integer> getKeyLength();

  void setKeyLength(ValueProvider<Integer> value);
}
