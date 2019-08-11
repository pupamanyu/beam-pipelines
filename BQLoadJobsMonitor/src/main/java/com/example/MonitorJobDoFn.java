package com.example;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
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

public class MonitorJobDoFn extends DoFn<String, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorJobDoFn.class);
  private static final BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
  private final FluentBackoff fluentBackoff;
  private final TupleTag<String> pushedBackForMonitoring;
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

  MonitorJobDoFn(Options options, TupleTag<String> pushedBackForMonitoring) {
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

  private void hasJobTransitionedToState(Job job, JobStatus.State toJobState)
      throws JobNotYetDoneException {
    if (job.getStatus().getState() != toJobState) {
      throw new JobNotYetDoneException(
          "BQ Job "
              + job.getJobId()
              + " has not transitioned from "
              + job.getStatus().getState()
              + " to "
              + toJobState
              + " yet.");
    }
  }

  private Job hasJobCompleted(String jobId)
      throws BigQueryException, IOException, InterruptedException, BackOffExhaustedException {
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
        hasJobTransitionedToState(job, JobStatus.State.DONE);
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
    String jobId = context.element();
    try {
      /*
       * Check if the Job is in DONE State
       */
      Job job = this.hasJobCompleted(jobId);
      /*
       * Job is in DONE State
       */
      this.processCompletedJob(job);
      /*
       * TODO: Update Job State, Job Statistics, Errors(if any) inside State Management
       */

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
       * Increment Counters
       */
      this.backOffExhaustedCount.inc();
      this.submittedForRetryCount.inc();
      context.output(this.pushedBackForMonitoring, context.element());
    } catch (IOException | InterruptedException e) {
      /*
       * Something bad happened during BackOff(Perhaps BackOff was interrupted)
       * Divert the incoming PubSub to Source PubSub for retry later
       * Use TupleTag with pushedBackForMonitoringJobs Tag
       * Increment Counters
       */
      this.backOffInterruptedCount.inc();
      this.submittedForRetryCount.inc();
      context.output(this.pushedBackForMonitoring, context.element());
    }
  }
}
