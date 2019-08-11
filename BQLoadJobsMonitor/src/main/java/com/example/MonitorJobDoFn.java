package com.example;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
import com.google.gson.Gson;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

public class MonitorJobDoFn extends DoFn<PubsubMessage, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorJobDoFn.class);
  private static final Gson GSON = new Gson();
  private static final BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
  private final FluentBackoff fluentBackoff;
  private final TupleTag<PubsubMessage> pushedBackForMonitoring;
  private Counter backOffExhaustedCount =
      Metrics.counter(MonitorJobDoFn.class, "backoff-exhausted");
  private Counter backOffInterruptedCount =
      Metrics.counter(MonitorJobDoFn.class, "backoff-interrupted");
  private Counter failedLoadJobCount = Metrics.counter(MonitorJobDoFn.class, "failed-load-jobs");
  private Counter successfulLoadJobCount =
      Metrics.counter(MonitorJobDoFn.class, "successful-load-jobs");
  private Counter submittedForRetryCount =
      Metrics.counter(MonitorJobDoFn.class, "submitted-for-retries");
  private Distribution jobAwaitingToRunLatencyMs =
      Metrics.distribution(MonitorJobDoFn.class, "job-awaiting-to-run-latency-ms");
  private Distribution jobRunLatencyMs =
      Metrics.distribution(MonitorJobDoFn.class, "job-run-latency-ms");
  private Distribution jobTotalLatencyMs =
      Metrics.distribution(MonitorJobDoFn.class, "job-total-latency-ms");

  MonitorJobDoFn(Options options, TupleTag<PubsubMessage> pushedBackForMonitoring) {
    /*
     * We are using Fluent BackOff so as to retry checking for Job in Running State
     * with an expectation that it may finish within BackOff expiry
     */
    this.fluentBackoff =
        FluentBackoff.DEFAULT
            .withExponent(options.getBackOffExponential().get())
            .withMaxRetries(options.getMaxRetries().get())
            .withInitialBackoff(Duration.standardSeconds(options.getInitialBackOffSeconds().get()));
    this.pushedBackForMonitoring = pushedBackForMonitoring;
  }

  private void updatePipelineMetrics(Job job) {
    JobStatistics jobStatistics = job.getStatistics();
    long jobStartTime = jobStatistics.getStartTime();
    long jobCreatedTime = jobStatistics.getCreationTime();
    long jobEndTime = jobStatistics.getEndTime();
    this.jobAwaitingToRunLatencyMs.update(jobStartTime - jobCreatedTime);
    this.jobRunLatencyMs.update(jobEndTime - jobStartTime);
    this.jobTotalLatencyMs.update((jobStartTime - jobCreatedTime) + (jobEndTime - jobStartTime));
    if (job.getStatus().getError() != null) {
      /*
       * Job has not Succeeded.
       * Error details are encapsulated inside Job in addition to the Job State, and Job Statistics
       */
      this.failedLoadJobCount.inc();
    } else {
      /*
       * Job is Successful. Update the Job State, Job Statistics
       */
      this.successfulLoadJobCount.inc();
    }
  }

  private void isLoadJobDone(Job job) throws JobNotYetDoneException {
    if (job.getStatus().getState() != JobStatus.State.DONE) {
      throw new JobNotYetDoneException(
          "BQ Job "
              + job.getJobId()
              + " has not transitioned from "
              + job.getStatus().getState()
              + " to "
              + JobStatus.State.DONE
              + " state yet.");
    }
  }

  private Job hasJobCompleted(String jobId)
      throws IOException, InterruptedException, BackOffExhaustedException {
    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff backOff = this.fluentBackoff.backoff();
    while (true) {
      try {
        Job job =
            bigQuery.getJob(
                jobId,
                BigQuery.JobOption.fields(
                    BigQuery.JobField.ID,
                    BigQuery.JobField.STATUS,
                    BigQuery.JobField.CONFIGURATION,
                    BigQuery.JobField.STATISTICS));
        isLoadJobDone(job);
        return job;
      } catch (JobNotYetDoneException j) {
        if (!BackOffUtils.next(sleeper, backOff)) {
          /*
           * Job did not change to the target State
           * BackOff Exhausted(Max Retries Reached)
           * Throw BackOffExhaustedException so we can take action on it.
           */
          throw new BackOffExhaustedException(j.getMessage());
        }
      }
    }
  }

  private void processCompletedJob(Job job) {
    /*
     * Update Pipeline Metrics
     */
    this.updatePipelineMetrics(job);
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    LoadRequest loadRequest =
        GSON.fromJson(
            new String(context.element().getPayload(), StandardCharsets.UTF_8), LoadRequest.class);
    Map<String, String> messageAttributes = context.element().getAttributeMap();
    String jobId = messageAttributes.get("jobId");
    try {
      /*
       * Check if the Job is in DONE State
       */
      messageAttributes.putIfAbsent(
          "jobMonitoringStartTimeMs", String.valueOf(Instant.now().toEpochMilli()));
      Job job = this.hasJobCompleted(jobId);
      /*
       * Job is in DONE State
       */
      this.processCompletedJob(job);
      /*
       * Add Job Monitoring Stats as PubSub attributes or Update State Management Store
       */
      messageAttributes.putIfAbsent(
          "jobMonitoringEndTimeMs", String.valueOf(Instant.now().toEpochMilli()));
      String jobMonitoringTotalTimeMs =
          String.valueOf(
              Integer.parseInt(messageAttributes.get("jobMonitoringEndTimeMs"))
                  + Integer.parseInt(messageAttributes.get("jobMonitoringStartTimeMs")));
      messageAttributes.putIfAbsent("jobMonitoringTotalTimeMs", jobMonitoringTotalTimeMs);
      messageAttributes.putIfAbsent("jobCompleted", "true");

      /*
       * TODO: Update Job State, Job Statistics, Errors(if any) inside State Management Store
       */
      long jobStartTime = job.getStatistics().getStartTime();
      long jobCreatedTime = job.getStatistics().getCreationTime();
      long jobEndTime = job.getStatistics().getEndTime();
      messageAttributes.putIfAbsent(
          "jobAwaitingToRunLatencyMs", String.valueOf(jobStartTime - jobCreatedTime));
      messageAttributes.putIfAbsent("jobRunLatencyMs", String.valueOf(jobEndTime - jobStartTime));
      messageAttributes.putIfAbsent(
          "jobTotalLatencyMs",
          String.valueOf((jobStartTime - jobCreatedTime) + (jobEndTime - jobStartTime)));
      if (job.getStatus().getError() != null) {
        /*
         * TODO: Job has not succeeded. Capture Error details and update state
         *  The size of Error Message and Error Reason may be large to fit
         *  Inside PubSub Attributes
         *  For eg:
         *  messageAttributes.putIfAbsent("errorMessage", job.getStatus().getError().getMessage());
         *  messageAttributes.putIfAbsent("errorReason", job.getStatus().getError().getReason());
         */
        messageAttributes.putIfAbsent("jobSuccessful", "false");
      } else {
        messageAttributes.putIfAbsent("jobSuccessful", "true");
      }
      /*
       * Send main output which is PCollection<Void> for Trivial Sink
       */
      context.output(null);
    } catch (BackOffExhaustedException e) {
      /*
       * Job did not transition to DONE State before BackOff expiry
       * Job is either in Pending State or in Running State
       * Divert the incoming PubSub to Source PubSub for retry later
       * Use TupleTag with pushedBackForMonitoringJobs Tag
       * Increment Counters, Increment pushedBackForMonitoringRetries Attribute value
       */
      messageAttributes.put(
          "pushedBackForMonitoringRetries",
          String.valueOf(
              Integer.parseInt(
                      messageAttributes.getOrDefault("pushedBackForMonitoringRetries", "1"))
                  + 1));
      this.backOffExhaustedCount.inc();
      this.submittedForRetryCount.inc();
      context.output(
          this.pushedBackForMonitoring,
          new PubsubMessage(context.element().getPayload(), messageAttributes));
    } catch (IOException | InterruptedException e) {
      /*
       * Something bad happened during BackOff(Perhaps BackOff was interrupted)
       * Divert the incoming PubSub to Source PubSub for retry later
       * Use TupleTag with pushedBackForMonitoringJobs Tag
       * Increment Counters, Increment pushedBackForMonitoringRetries Attribute value
       */
      messageAttributes.put(
          "pushedBackForMonitoringRetries",
          String.valueOf(
              Integer.parseInt(
                      messageAttributes.getOrDefault("pushedBackForMonitoringRetries", "1"))
                  + 1));
      this.backOffInterruptedCount.inc();
      this.submittedForRetryCount.inc();
      context.output(
          this.pushedBackForMonitoring,
          new PubsubMessage(context.element().getPayload(), messageAttributes));
    }
  }
}
