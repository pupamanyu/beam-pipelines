package com.example;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface Options extends PipelineOptions, DataflowPipelineOptions {

  @Description("Source PubSub Subscription for Load Requests")
  @Validation.Required
  ValueProvider<String> getSourceSubscription();

  void setSourceSubscription(ValueProvider<String> value);

  @Description("Source PubSub Topic for Load Requests")
  @Validation.Required
  ValueProvider<String> getSourceTopic();

  void setSourceTopic(ValueProvider<String> value);

  @Description("BQ Project Name")
  @Validation.Required
  ValueProvider<String> getBQProject();

  void setBQProject(ValueProvider<String> value);

  @Description("BQ DataSet Name")
  @Validation.Required
  ValueProvider<String> getDataSet();

  void setDataSet(ValueProvider<String> value);

  @Description("BQ Table Name")
  @Validation.Required
  ValueProvider<String> getTableName();

  void setTableName(ValueProvider<String> value);

  @Description("Initial BackOff in Seconds for checking BQ Job Queue")
  @Validation.Required
  ValueProvider<Integer> getInitialBackOffSeconds();

  void setInitialBackOffSeconds(ValueProvider<Integer> value);

  @Description("BackOff Exponential in Seconds for checking BQ Job Queue")
  @Validation.Required
  ValueProvider<Float> getBackOffExponential();

  void setBackOffExponential(ValueProvider<Float> value);

  @Description("Maximum Retries for checking BQ Job Queue")
  @Validation.Required
  ValueProvider<Integer> getMaxRetries();

  void setMaxRetries(ValueProvider<Integer> value);

  @Description("Pending Jobs Threshold for the BQ Job Queue")
  @Validation.Required
  ValueProvider<Integer> getPendingJobThreshold();

  void setPendingJobThreshold(ValueProvider<Integer> value);
}
