package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;

public class BQLoader {

  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    final TupleTag<PubsubMessage> submittedLoadJobs = new TupleTag<PubsubMessage>();
    final TupleTag<PubsubMessage> submittedForRetryLoadJobs = new TupleTag<PubsubMessage>();
    final TupleTag<String> failedSubmissionLoadJobs = new TupleTag<String>();
    final TupleTag<Void> voidTupleTag = new TupleTag<Void>();

    PCollectionTuple pCollectionTuple =
        p.apply(
                "Consume Load Requests",
                PubsubIO.readMessagesWithAttributes()
                    .withIdAttribute(options.getSourceDeDupAttribute().get())
                    .fromSubscription(options.getSourceSubscription()))
            .apply(
                "Submit BQ Load Jobs",
                ParDo.of(
                        new BQLoaderDoFn(
                            options,
                            submittedLoadJobs,
                            submittedForRetryLoadJobs,
                            failedSubmissionLoadJobs))
                    .withOutputTags(
                        voidTupleTag,
                        TupleTagList.of(submittedLoadJobs).and(submittedForRetryLoadJobs)));

    /*
     * Extract JobId from PCollection<KV<String, String>> for
     * feeding it to Job Monitoring PubSub Topic. Perhaps another
     * Pipeline consuming from the JobMonitoring PubSub Topic can
     * assume the responsibility of monitoring the Jobs
     * until their end state
     */
    pCollectionTuple
        .get(submittedLoadJobs)
        .apply(
            "Feed the Pending/Running Load Jobs back for Monitoring Later",
            PubsubIO.writeMessages().to(options.getJobMonitoringTopic().get()).withIdAttribute(options.getTargetDeDupAttribute().get()));

    /*
     * Extract The Jobs for Retry Submission to the Source PubSub Topic
     * Adding DeDup on the Attribute may cause LoadRequests to be Dropped
     * from Retry. Caution needs to be exercised here for DeDup here. Correct State changes
     * in State DB can help in avoiding concurrent retries.
     */
    pCollectionTuple
        .get(submittedForRetryLoadJobs)
        .apply(
            "Inject Load Requests to Source Queue",
            PubsubIO.writeMessages().to(options.getSourceTopic().get()).withIdAttribute(options.getSourceDeDupAttribute().get()));

    /*
     * Store the Failed Load Requests into Dead Letter GCS after Allowed Max Retries
     * for Manual Inspection
     */
    pCollectionTuple
        .get(failedSubmissionLoadJobs)
        .apply(
            "Window the Failed Load Requests into Fixed Windows",
            Window.<String>into(
                    FixedWindows.of(
                        Duration.standardMinutes(options.getDeadLetterGCSWindowDuration().get())))
                .triggering(
                    AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(
                            AfterPane.elementCountAtLeast(
                                options.getDeadLetterMaxElementCount().get()))
                        .withLateFirings(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(
                                    Duration.standardMinutes(
                                        options.getDeadLetterGCSAllowedLateness().get()))))
                .withAllowedLateness(
                    Duration.standardMinutes(options.getDeadLetterGCSAllowedLateness().get()))
                .discardingFiredPanes())
        .apply(
            "Save Failed Load Requests for Manual Inspection",
            TextIO.write()
                .to(
                    "gs://"
                        + options.getDeadLetterGCSBucket().get()
                        + "/"
                        + options.getDeadLetterGCSPrefix().get())
                .withWindowedWrites());
    /*
     * Trivial Sink for Main Output(PCollection<Void>)
     */
    pCollectionTuple.get(voidTupleTag).apply("Trivial Sink", new LoaderTrivialSink());

    p.run();
  }
}
