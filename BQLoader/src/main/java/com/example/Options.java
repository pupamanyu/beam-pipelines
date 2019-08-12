package com.example;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
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

  @Description("Source PubSub DeDup Attribute for Load Requests")
  @Validation.Required
  ValueProvider<String> getSourceDeDupAttribute();

  void setSourceDeDupAttribute(ValueProvider<String> value);

  @Description("Target PubSub Topic for Pushing Submitted Jobs for Monitoring")
  @Validation.Required
  ValueProvider<String> getJobMonitoringTopic();

  void setJobMonitoringTopic(ValueProvider<String> value);

  @Description("Target PubSub DeDup Attribute for Pushing Submitted Jobs for Monitoring")
  @Validation.Required
  ValueProvider<String> getTargetDeDupAttribute();

  void setTargetDeDupAttribute(ValueProvider<String> value);

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

  @Description("Concurrent Load Jobs Threshold for the BQ Job Queue(Depends on BQ Slots)")
  @Validation.Required
  ValueProvider<Integer> getConcurrentLoadJobsThreshold();

  void setConcurrentLoadJobsThreshold(ValueProvider<Integer> value);

  @Description(
      "Concurrent Load Jobs Cache TTL in minutes. This is used to reduce number of calls to List Running BQ Jobs")
  @Validation.Required
  ValueProvider<Integer> getConcurrentLoadJobsCacheTTLMinutes();

  void setConcurrentLoadJobsCacheTTLMinutes(ValueProvider<Integer> value);

  @Description(
      "Load Job Submission Retries through the Retry Cycle. This acts as pressure valve. Set this to a higher number(like > 10000). Lower number may cause false negatives. This is used to determine the maximum amount of attempts for the Load Request to go through retry cycle(Source PubSub) before considering the Load Request for manual inspection")
  @Default.Integer(10000)
  ValueProvider<Integer> getMaxLoadJobRetryCycles();

  void setMaxLoadJobRetryCycles(ValueProvider<Integer> value);

  @Description(
      "Dead Letter GCS Bucket Name without gs:// for Saving Failed Load Requests after specified maximum job submission attempts")
  @Validation.Required
  ValueProvider<String> getDeadLetterGCSBucket();

  void setDeadLetterGCSBucket(ValueProvider<String> value);

  @Description(
      "Dead Letter GCS File Prefix for Saving Failed Load Requests after specified maximum job submission attempts")
  @Validation.Required
  ValueProvider<String> getDeadLetterGCSPrefix();

  void setDeadLetterGCSPrefix(ValueProvider<String> value);

  @Description(
      "Fixed Window Duration in minutes for the writing Dead Letter Files within GCS. Defaults to 5 minutes")
  @Default.Integer(5)
  ValueProvider<Integer> getDeadLetterGCSWindowDuration();

  void setDeadLetterGCSWindowDuration(ValueProvider<Integer> value);

  @Description(
      "Allowed Lateness Duration in minutes for the writing Dead Letter Files within GCS. Defaults to 2 minutes")
  @Default.Integer(2)
  ValueProvider<Integer> getDeadLetterGCSAllowedLateness();

  void setDeadLetterGCSAllowedLateness(ValueProvider<Integer> value);

  @Description(
      "Early Firing Element Count for writing the Dead Letter Files within GCS to handle surge. Defaults to 10000")
  @Default.Integer(10000)
  ValueProvider<Integer> getDeadLetterMaxElementCount();

  void setDeadLetterMaxElementCount(ValueProvider<Integer> value);
}
