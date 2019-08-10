package com.example;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.gson.Gson;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;

public class BQLoaderDoFn extends DoFn<String, Void> {

  private static final BigQuery.JobListOption JOB_LIST_STATE_FILTER =
      BigQuery.JobListOption.stateFilter(JobStatus.State.RUNNING);
  private static final BigQuery.JobListOption JOB_LIST_ALL_USERS_FILTER =
      BigQuery.JobListOption.allUsers();
  private static final Gson GSON = new Gson();
  private final int concurrentLoadJobsThreshold;
  private final FluentBackoff fluentBackoff;
  private final TupleTag<KV<String, String>> submittedLoadJobs;
  private final TupleTag<String> submittedForRetryLoadJobs;
  private BigQuery bigQuery;
  private Counter submittedCount = Metrics.counter(BQLoaderDoFn.class, "submittedCount");
  private Counter backOffExhaustedCount =
      Metrics.counter(BQLoaderDoFn.class, "backOffExhaustedCount");
  private Counter bigQueryExceptionCount =
      Metrics.counter(BQLoaderDoFn.class, "bigQueryExceptionCount");
  private Counter backOffInterruptedCount =
      Metrics.counter(BQLoaderDoFn.class, "backOffInterruptedCount");
  private Counter submittedForRetryCount =
      Metrics.counter(BQLoaderDoFn.class, "submittedForRetryCount");

  BQLoaderDoFn(
      Options options,
      TupleTag<KV<String, String>> submittedLoadJobs,
      TupleTag<String> submittedForRetryLoadJobs) {
    this.fluentBackoff =
        FluentBackoff.DEFAULT
            .withExponent(options.getBackOffExponential().get())
            .withMaxRetries(options.getMaxRetries().get())
            .withInitialBackoff(Duration.standardSeconds(options.getInitialBackOffSeconds().get()));
    this.concurrentLoadJobsThreshold = options.getConcurrentLoadJobsThreshold().get();
    this.submittedLoadJobs = submittedLoadJobs;
    this.submittedForRetryLoadJobs = submittedForRetryLoadJobs;
  }

  private void isQueueUnderThreshold() throws JobsThresholdException {
    LinkedHashSet<JobId> concurrentLoadJobs = new LinkedHashSet<JobId>();
    Page<Job> jobs = this.bigQuery.listJobs(JOB_LIST_STATE_FILTER, JOB_LIST_ALL_USERS_FILTER);
    for (Job job : jobs.iterateAll()) {
      concurrentLoadJobs.add(job.getJobId());
    }
    if (!(concurrentLoadJobs.size() < this.concurrentLoadJobsThreshold)) {
      throw new JobsThresholdException(
          "BQ Job Queue is beyond the set threshold of " + this.concurrentLoadJobsThreshold);
    }
  }

  private LoadJobConfiguration getLoadJobConfiguration(
      TableId destinationTableId, String sourceUri) {
    LoadJobConfiguration.Builder loadJobConfigurationBuilder =
        LoadJobConfiguration.newBuilder(destinationTableId, sourceUri);
    loadJobConfigurationBuilder
        .setFormatOptions(FormatOptions.avro())
        .setUseAvroLogicalTypes(true)
        .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
        .setSchemaUpdateOptions(
            Arrays.asList(
                JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION));
    return loadJobConfigurationBuilder.build();
  }

  private Job submitJob(TableId destinationTableId, String sourceUri)
      throws BigQueryException, IOException, InterruptedException, BackOffExhaustedException {
    Job job = null;
    LoadJobConfiguration loadJobConfiguration =
        getLoadJobConfiguration(destinationTableId, sourceUri);
    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff backOff = this.fluentBackoff.backoff();
    while (true) {
      try {
        isQueueUnderThreshold();
        job = bigQuery.create(JobInfo.of(loadJobConfiguration));
        return job;
      } catch (JobsThresholdException q) {
        if (!BackOffUtils.next(sleeper, backOff)) {
          /*
           * Failed to get the Open Spot in the Load Job Queue.
           * BackOff Exhausted(Max Retries Reached)
           * Throw BackOffExhaustedException so we can take action on it.
           */
          throw new BackOffExhaustedException(q.getMessage());
        }
      }
    }
  }

  @Setup
  public void doSetup() {

    this.bigQuery = BigQueryOptions.getDefaultInstance().getService();
  }

  @ProcessElement
  public void processElement(ProcessContext context) {

    LoadRequest loadRequest = GSON.fromJson(context.element(), LoadRequest.class);
    String bundlePrefixPath = loadRequest.payload.bundlePrefixPath;
    String bundleDataset = loadRequest.payload.bundleDataset;
    String bundleTable = loadRequest.payload.bundleTable;
    String bundleProject = loadRequest.payload.bundleProject;
    try {
      Job job = submitJob(TableId.of(bundleProject, bundleDataset, bundleTable), bundlePrefixPath);
      this.submittedCount.inc();
      /* Load Job Submitted Successfully
       * Send the KV<JobId, String> to be processed for state db insertions
       * We need to insert JobId along with input Load Request JSON with possibly other metadata
       * Can feed the State DB Load Job Injector using TupleTag loadJobStateData TupleTag
       */
      context.output(this.submittedLoadJobs, KV.of(job.getJobId().toString(), context.element()));
    } catch (BigQueryException e) {
      /*
       * Something bad happened while submitting Load job to BQ
       * Perhaps a service error
       * Divert the Incoming PubSub to Source PubSub for retry later
       * Use TupleTag with submittedForRetryLoadJobs Tag
       * Increment Counters
       */
      this.submittedForRetryCount.inc();
      this.bigQueryExceptionCount.inc();
      context.output(this.submittedForRetryLoadJobs, context.element());
    } catch (BackOffExhaustedException e) {
      /*
       * Concurrent Jobs in the BQ Load Queue is beyond the threshold
       * Divert the Incoming PubSub to Source PubSub for retry later
       * Use TupleTag with submittedForRetryLoadJobs Tag
       * Increment Counters
       */
      this.backOffExhaustedCount.inc();
      this.submittedForRetryCount.inc();
      context.output(this.submittedForRetryLoadJobs, context.element());
    } catch (IOException | InterruptedException e) {
      /*
       * Something bad happened during BackOff(Perhaps BackOff was interrupted)
       * Divert the incoming PubSub to Source PubSub for retry later
       * Use TupleTag with submittedForRetryLoadJobs Tag
       */
      this.backOffInterruptedCount.inc();
      this.submittedForRetryCount.inc();
      context.output(this.submittedForRetryLoadJobs, context.element());
    }
  }
}
