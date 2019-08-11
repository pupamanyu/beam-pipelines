package com.example;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class BQLoaderDoFn extends DoFn<String, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(BQLoaderDoFn.class);

  private static final BigQuery.JobListOption JOB_LIST_RUNNING_FILTER =
      BigQuery.JobListOption.stateFilter(JobStatus.State.RUNNING);
  private static final BigQuery.JobListOption JOB_LIST_ALL_USERS_FILTER =
      BigQuery.JobListOption.allUsers();
  private static final Gson GSON = new Gson();
  private static final BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
  private final int concurrentLoadJobsThreshold;
  private final int loadJobsCacheTTLMinutes;
  private final FluentBackoff fluentBackoff;
  private final TupleTag<KV<String, String>> submittedLoadJobs;
  private final TupleTag<String> submittedForRetryLoadJobs;
  private Counter submittedCount = Metrics.counter(BQLoaderDoFn.class, "submittedCount");
  private Counter backOffExhaustedCount =
      Metrics.counter(BQLoaderDoFn.class, "loader-backoff-exhausted-count");
  private Counter bigQueryExceptionCount =
      Metrics.counter(BQLoaderDoFn.class, "loader-bigquery-exception-count");
  private Counter backOffInterruptedCount =
      Metrics.counter(BQLoaderDoFn.class, "loader-backoff-interrupted-count");
  private Counter submittedForRetryCount =
      Metrics.counter(BQLoaderDoFn.class, "loader-submitted-for-retry-count");
  private Distribution jobSubmissionLatencyMs =
      Metrics.distribution(BQLoaderDoFn.class, "load-job-submission-latency-ms");

  BQLoaderDoFn(
      Options options,
      TupleTag<KV<String, String>> submittedLoadJobs,
      TupleTag<String> submittedForRetryLoadJobs) {
    /*
     * We are using Fluent BackOff for Throttling the Job Submission Rate
     * so as to ensure, we do not have BQ Jobs piled up in Pending State
     */
    this.fluentBackoff =
        FluentBackoff.DEFAULT
            .withExponent(options.getBackOffExponential().get())
            .withMaxRetries(options.getMaxRetries().get())
            .withInitialBackoff(Duration.standardSeconds(options.getInitialBackOffSeconds().get()));
    this.concurrentLoadJobsThreshold = options.getConcurrentLoadJobsThreshold().get();
    this.loadJobsCacheTTLMinutes = options.getConcurrentLoadJobsCacheTTLMinutes().get();
    this.submittedLoadJobs = submittedLoadJobs;
    this.submittedForRetryLoadJobs = submittedForRetryLoadJobs;
  }

  private LinkedHashSet<Job> getRunningLoadJobs(int queueSize, int queueTTLMinutes)
      throws ExecutionException {
    /**
     * We are using Guava LoadingCache to reduce spamming BQ for getting the list of Running
     * Jobs(Concurrent). TTL is configurable. Size of the LinkedHashSet to store Running Jobs is
     * same as Threshold for Concurrent Jobs.
     */
    /*
     * Using Single Fixed Key for storing the Running Jobs List in a Set
     */
    String cacheKey = "runningLoadJobs";
    LoadingCache<String, LinkedHashSet<Job>> linkedHashSetLoadingCache =
        CacheBuilder.newBuilder()
            .maximumSize(queueSize)
            .expireAfterWrite(queueTTLMinutes, TimeUnit.MINUTES)
            .build(
                new CacheLoader<String, LinkedHashSet<Job>>() {
                  @Override
                  public LinkedHashSet<Job> load(String cacheKey) throws Exception {
                    LinkedHashSet<Job> concurrentLoadJobs = new LinkedHashSet<Job>();
                    Page<Job> jobs =
                        bigQuery.listJobs(JOB_LIST_RUNNING_FILTER, JOB_LIST_ALL_USERS_FILTER);
                    for (Job job : jobs.iterateAll()) {
                      concurrentLoadJobs.add(job);
                    }
                    return concurrentLoadJobs;
                  }
                });
    return linkedHashSetLoadingCache.get(cacheKey);
  }

  private void isQueueUnderThreshold(int queueSize, int queueTTLMinutes)
      throws ExecutionException, JobsThresholdException {
    if (!(getRunningLoadJobs(queueSize, queueTTLMinutes).size()
        < this.concurrentLoadJobsThreshold)) {
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
        isQueueUnderThreshold(this.concurrentLoadJobsThreshold, this.loadJobsCacheTTLMinutes);
        job = bigQuery.create(JobInfo.of(loadJobConfiguration));
        return job;
      } catch (JobsThresholdException | ExecutionException q) {
        if (!BackOffUtils.next(sleeper, backOff)) {
          /*
           * Failed to get the Open Spot in the Load Job Queue.
           * Right now ExecutionException arising out of LoadingCache is also handled
           * the same way as JobsThresholdException.
           * BackOff Exhausted(Max Retries Reached)
           * Throw BackOffExhaustedException so we can take action on it.
           */
          throw new BackOffExhaustedException(q.getMessage());
        }
      }
    }
  }

  @ProcessElement
  public void processElement(ProcessContext context) {

    /*
     * Take note of Time for measuring the Job Submission Latency(ms)
     */
    long loadStartTimeMs = Instant.now().toEpochMilli();
    LoadRequest loadRequest = GSON.fromJson(context.element(), LoadRequest.class);
    String bundlePrefixPath = loadRequest.payload.bundlePrefixPath;
    String bundleDataset = loadRequest.payload.bundleDataset;
    String bundleTable = loadRequest.payload.bundleTable;
    String bundleProject = loadRequest.payload.bundleProject;
    try {
      Job job = submitJob(TableId.of(bundleProject, bundleDataset, bundleTable), bundlePrefixPath);
      long jobSubmittedTimeMs = Instant.now().toEpochMilli();
      this.submittedCount.inc();
      /*
       * Update the Metrics to record the Job Submission Latency(ms)
       */
      this.jobSubmissionLatencyMs.update(jobSubmittedTimeMs - loadStartTimeMs);
      /* Load Job Submitted Successfully
       * Send the KV<JobId, String> to be processed for state db insertions
       * We need to insert JobId along with input Load Request JSON with possibly other metadata
       * Can feed the State DB Load Job Injector using TupleTag loadJobStateData TupleTag
       */
      context.output(this.submittedLoadJobs, KV.of(job.getJobId().toString(), context.element()));
      /*
       * Send main output which is PCollection<Void>
       */
      context.output(null);
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
