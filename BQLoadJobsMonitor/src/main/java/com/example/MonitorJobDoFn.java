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
import org.apache.commons.lang3.ObjectUtils;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MonitorJobDoFn extends DoFn<PubsubMessage, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorJobDoFn.class);
  private static final Gson GSON = new Gson();
  private static final BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
  private final FluentBackoff fluentBackoff;
  private final Integer maxLoadRequestJobFailures;
  private final TupleTag<PubsubMessage> pushedBackForMonitoring;
  private final TupleTag<PubsubMessage> pushedBackForRetryAfterJobFailure;
  private final TupleTag<String> failedLoadJobRequests;
  private Counter backOffExhaustedCount =
      Metrics.counter(MonitorJobDoFn.class, "backoff-exhausted");
  private Counter backOffInterruptedCount =
      Metrics.counter(MonitorJobDoFn.class, "backoff-interrupted");
  private Counter failedLoadJobCount = Metrics.counter(MonitorJobDoFn.class, "failed-load-jobs");
  private Counter successfulLoadJobCount =
      Metrics.counter(MonitorJobDoFn.class, "successful-load-jobs");
  private Counter submittedForMonitoringRetryCount =
      Metrics.counter(MonitorJobDoFn.class, "submitted-for-monitoring-retries");
  private Distribution jobAwaitingToRunLatencyMs =
      Metrics.distribution(MonitorJobDoFn.class, "job-awaiting-to-run-latency-ms");
  private Distribution jobRunLatencyMs =
      Metrics.distribution(MonitorJobDoFn.class, "job-run-latency-ms");
  private Distribution jobTotalLatencyMs =
      Metrics.distribution(MonitorJobDoFn.class, "job-total-latency-ms");

  MonitorJobDoFn(Options options, TupleTag<PubsubMessage> pushedBackForMonitoring, TupleTag<PubsubMessage> pushedBackForRetryAfterJobFailure, TupleTag<String> failedLoadJobRequests) {
    /*
     * We are using Fluent BackOff so as to retry checking for Job in Running State
     * with an expectation that it may finish within BackOff expiry
     */
    this.fluentBackoff =
        FluentBackoff.DEFAULT
            .withExponent(options.getBackOffExponential().get())
            .withMaxRetries(options.getMaxRetries().get())
            .withInitialBackoff(Duration.standardSeconds(options.getInitialBackOffSeconds().get()));
    this.maxLoadRequestJobFailures = options.getMaxLoadRequestJobFailures().get();
    this.pushedBackForMonitoring = pushedBackForMonitoring;
    this.pushedBackForRetryAfterJobFailure = pushedBackForRetryAfterJobFailure;
    this.failedLoadJobRequests = failedLoadJobRequests;
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

  private void updateCompletedJobMetrics(Job job) {
    /*
     * Update Pipeline Metrics
     */
    this.updatePipelineMetrics(job);
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    LoaderEnvelope loaderEnvelope =
        GSON.fromJson(
            new String(context.element().getPayload(), StandardCharsets.UTF_8), LoaderEnvelope.class);

    String jobId = loaderEnvelope.loaderEnvelopeAttributes.getJobId();
    try {
      /*
       * Check if the Job is in DONE State
       */
      loaderEnvelope.loaderEnvelopeAttributes.setJobCreatedTimestamp(ObjectUtils.firstNonNull(loaderEnvelope.loaderEnvelopeAttributes.getJobMonitoringStartTimeStamp(),Instant.now().toEpochMilli()));
      Job job = this.hasJobCompleted(jobId);
      /*
       * Job is in DONE State
       */
      this.updateCompletedJobMetrics(job);
      /*
       * Add Job Monitoring Stats as PubSub attributes or Update State Management Store
       */
      loaderEnvelope.loaderEnvelopeAttributes.setJobMonitoringEndTimeStamp(ObjectUtils.firstNonNull(loaderEnvelope.loaderEnvelopeAttributes.getJobMonitoringEndTimeStamp(),Instant.now().toEpochMilli()));
      loaderEnvelope.loaderEnvelopeAttributes.setJobMonitoringTotalTimeMs(ObjectUtils.firstNonNull(loaderEnvelope.loaderEnvelopeAttributes.getJobMonitoringTotalTimeMs(), (
          loaderEnvelope.loaderEnvelopeAttributes.getJobMonitoringEndTimeStamp())
              + loaderEnvelope.loaderEnvelopeAttributes.getJobMonitoringStartTimeStamp()));
      loaderEnvelope.loaderEnvelopeAttributes.setJobCompleted(true);

      /*
       * TODO: Update Job State, Job Statistics, Errors(if any) inside State Management Store
       */
      long jobStartTime = job.getStatistics().getStartTime();
      long jobCreatedTime = job.getStatistics().getCreationTime();
      long jobEndTime = job.getStatistics().getEndTime();
      loaderEnvelope.loaderEnvelopeAttributes.setJobAwaitingToRunLatencyMs(jobStartTime - jobCreatedTime);
      loaderEnvelope.loaderEnvelopeAttributes.setJobRunLatencyMs(jobEndTime - jobStartTime);
      loaderEnvelope.loaderEnvelopeAttributes.setJobTotalLatencyMs((jobStartTime - jobCreatedTime) + (jobEndTime - jobStartTime));
      /*
       * TODO: We will lose LoaderEnvelope if the job has failed. Ensure that Job Statistics are updated before
       *  moving forward
       */
      if (job.getStatus().getError() != null) {
        /*
         * TODO: The Job has Failed. Capture Error details and update state
         *  The size of Error Message and Error Reason may be large to fit
         *  Inside PubSub Attributes
         *  For eg:
         *  messageAttributes.putIfAbsent("errorMessage", job.getStatus().getError().getMessage());
         *  messageAttributes.putIfAbsent("errorReason", job.getStatus().getError().getReason());
         *
         * Extract and Submit Original LoadRequest(input element) with Attributes to pushedBackForRetryAfterJobFailure TupleTag
         * for Job Replay(Retry after Job Failure within BQ)
         */
        LoadRequest loadRequest = loaderEnvelope.loadRequest;
        Integer retriesSoFar = ObjectUtils.firstNonNull(loaderEnvelope.loadRequest.loadRequestAttributes.getRetryAttemptsAfterJobFailures(), 0);
        if (retriesSoFar < this.maxLoadRequestJobFailures) {
          /*
           * Check if we are within the max allowed number of RetryAttemptsAfterJobFailures
           *  and submit the Load Request for Retry
           */
           loadRequest.loadRequestAttributes.setRetryAttemptsAfterJobFailures(retriesSoFar + 1);
           List<String> previousJobIds = ObjectUtils.firstNonNull(loadRequest.loadRequestAttributes.getPreviousFailedJobIds(), new ArrayList<String>());
           previousJobIds.add(job.getJobId().toString());
           loadRequest.loadRequestAttributes.setPreviousFailedJobIds(previousJobIds);
           Map<String, String> loadRequestAttributesMap = new HashMap<String, String>();
          loadRequestAttributesMap.put("uniqueMessageId", UUID.randomUUID().toString());
           context.output(this.pushedBackForRetryAfterJobFailure, new PubsubMessage(GSON.toJson(loadRequest).getBytes(StandardCharsets.UTF_8), loadRequestAttributesMap));
        } else {
          /*
           * We are beyond the maxLoadRequestJobFailures threshold. We need to send the LoadRequest to Dead Letter GCS location
           * for Manual Inspection/Rectification
           */
          context.output(this.failedLoadJobRequests, GSON.toJson(loadRequest));
        }
      } else {
        /*
         * TODO: The Job has Succeeded. Extract Stats from LoadEnvelopeAttributes, and Job Status
         *  Update State Management and we are done monitoring the Job here
         */
      }
    } catch (BackOffExhaustedException e) {
      /*
       * Job did not transition to DONE State before BackOff expiry
       * Job is either in Pending State or in Running State
       * Divert the incoming PubSub to Source PubSub for retry later
       * Use TupleTag with pushedBackForMonitoringJobs Tag
       * Increment Counters, Increment pushedBackForMonitoringRetries Attribute value
       */
      this.backOffExhaustedCount.inc();
      this.submittedForMonitoringRetryCount.inc();
      loaderEnvelope.loaderEnvelopeAttributes.setPushedBackForMonitoringRetries(ObjectUtils.firstNonNull(loaderEnvelope.loaderEnvelopeAttributes.getPushedBackForMonitoringRetries(), 0) + 1);
      Map<String, String> loaderEnvelopeAttributesMap = new HashMap<String, String>();
      loaderEnvelopeAttributesMap.put("uniqueMessageId", UUID.randomUUID().toString());
      context.output(
          this.pushedBackForMonitoring,
          new PubsubMessage(GSON.toJson(loaderEnvelope).getBytes(StandardCharsets.UTF_8), loaderEnvelopeAttributesMap));
    } catch (IOException | InterruptedException e) {
      /*
       * Something bad happened during BackOff(Perhaps BackOff was interrupted)
       * Divert the incoming PubSub to Source PubSub for retry later
       * Use TupleTag with pushedBackForMonitoringJobs Tag
       * Increment Counters, Increment pushedBackForMonitoringRetries Attribute value
       */
      this.backOffInterruptedCount.inc();
      this.submittedForMonitoringRetryCount.inc();
      loaderEnvelope.loaderEnvelopeAttributes.setPushedBackForMonitoringRetries(ObjectUtils.firstNonNull(loaderEnvelope.loaderEnvelopeAttributes.getPushedBackForMonitoringRetries(), 0) + 1);
      Map<String, String> loaderEnvelopeAttributesMap = new HashMap<String, String>();
      loaderEnvelopeAttributesMap.put("uniqueMessageId", UUID.randomUUID().toString());
      context.output(
          this.pushedBackForMonitoring,
          new PubsubMessage(GSON.toJson(loaderEnvelope).getBytes(StandardCharsets.UTF_8), loaderEnvelopeAttributesMap));
    }
  }
}
