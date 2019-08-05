package com.example;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface Options extends PipelineOptions, DataflowPipelineOptions {

  @Description("PubSub Subscription for GCS File Notifications")
  @Validation.Required
  ValueProvider<String> getSubscription();

  void setSubscription(ValueProvider<String> value);

  @Description("Max Bundle Size in Bytes")
  @Validation.Required
  ValueProvider<Long> getBundleSizeBytes();

  void setBundleSizeBytes(ValueProvider<Long> value);

  @Description("Max Duration in Minutes to Wait for the Bundle to finalize")
  @Validation.Required
  ValueProvider<Integer> getStalenessMinutes();

  void setStalenessMinutes(ValueProvider<Integer> value);

  @Description("Fixed Window Duration in Minutes")
  @Validation.Required
  ValueProvider<Integer> getWindowDurationMinutes();

  void setWindowDurationMinutes(ValueProvider<Integer> value);

  @Description("Threshold(Number of Elements) for Early Firing to handle Burst Traffic")
  @Validation.Required
  ValueProvider<Integer> getEarlyFiringElementsThreshold();

  void setEarlyFiringElementsThreshold(ValueProvider<Integer> value);

  @Description("Maximum Number of Elements in a Bundle")
  @Validation.Required
  ValueProvider<Integer> getBundleElementsThreshold();

  void setBundleElementsThreshold(ValueProvider<Integer> value);

  @Description("Allowed Lateness Duration in Minutes for the Window")
  @Validation.Required
  ValueProvider<Integer> getAllowedLatenessMinutes();

  void setAllowedLatenessMinutes(ValueProvider<Integer> value);

  @Description("Catalog for the Bundle to Finalize")
  @Validation.Required
  ValueProvider<String> getCatalog();

  void setCatalog(ValueProvider<String> value);

  @Description("Table for the Bundle to Finalize")
  @Validation.Required
  ValueProvider<String> getTable();

  void setTable(ValueProvider<String> value);

  @Description("Pipeline Output Sink File Prefix")
  @Validation.Required
  ValueProvider<String> getOutputPrefix();

  void setOutputPrefix(ValueProvider<String> value);

  @Description("Pipeline Error Sink File Prefix")
  @Validation.Required
  ValueProvider<String> getErrorOutputPrefix();

  void setErrorOutputPrefix(ValueProvider<String> value);
}
