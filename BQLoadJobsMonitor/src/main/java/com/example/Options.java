package com.example;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface Options extends PipelineOptions, DataflowPipelineOptions {

  @Description("Source PubSub Subscription for Successfully Submitted Load Requests")
  @Validation.Required
  ValueProvider<String> getSourceSubscription();

  void setSourceSubscription(ValueProvider<String> value);

  @Description("Source PubSub Topic for Successfully Submitted Load Requests")
  @Validation.Required
  ValueProvider<String> getSourceTopic();

  void setSourceTopic(ValueProvider<String> value);

  @Description("Source PubSub DeDup ID Attribute for Successfully Submitted Load Requests")

  @Default.String("uniqueMessageId")
  ValueProvider<String> getSourceDeDupId();

  void setSourceDeDupID(ValueProvider<String> value);

  @Description("Target PubSub DeDup ID Attribute for pushing the Retry of Jobs which were submitted and ended in Failed State")
  @Default.String("uniqueMessageId")
  ValueProvider<String> getRetryAfterJobFailureDeDupId();

  void setRetryAfterJobFailureDeDupID(ValueProvider<String> value);

  @Description("Target PubSub Topic for pushing the Retry of Jobs which were submitted and ended in Failed State")
  @Validation.Required
  ValueProvider<String> getRetryAfterJobFailureTopic();

  void setRetryAfterJobFailureTopic(ValueProvider<String> value);

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

  @Description("Maximum Retries for BQ Load Request to be considered unrecoverable so that we can divert the Load Request for manual inspection. Defaults to 3 Job Failures after successful BQ Job Submission")
  @Default.Integer(3)
  ValueProvider<Integer> getMaxLoadRequestJobFailures();

  void setMaxLoadRequestJobFailures(ValueProvider<Integer> value);

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
