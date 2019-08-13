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

public class LoadJobsMonitor {

  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    final TupleTag<PubsubMessage> pushedBackForMonitoring = new TupleTag<PubsubMessage>();
    final TupleTag<PubsubMessage> pushedBackForRetryAfterJobFailure = new TupleTag<PubsubMessage>();
    final TupleTag<String> failedLoadJobRequests = new TupleTag<String>();

    final TupleTag<Void> voidTupleTag = new TupleTag<Void>();

    PCollectionTuple pCollectionTuple =
        p.apply(
                "Consume BQ Load Jobs to be Monitored",
                PubsubIO.readMessagesWithAttributes()
                    .fromSubscription(options.getSourceSubscription()))
            .apply(
                "Monitor BQ Load Jobs",
                ParDo.of(new MonitorJobDoFn(options, pushedBackForMonitoring, pushedBackForRetryAfterJobFailure, failedLoadJobRequests))
                    .withOutputTags(voidTupleTag, TupleTagList.of(pushedBackForMonitoring).and(pushedBackForRetryAfterJobFailure).and(failedLoadJobRequests)));

    /*
     * Resubmit Jobs for Monitoring post Back Off Exhaustion
     *
     */
    pCollectionTuple
        .get(pushedBackForMonitoring)
        .apply(
            "Push Back BQ Load Jobs for Monitoring",
            PubsubIO.writeMessages()
                .to(options.getSourceTopic().get()));

    /*
     * Resubmit Load Job Request after all the previous attempt(s) so far
     * resulted in BQ Job Failure and when number of such
     * failed attempts < maxLoadRequestJobFailures
     *
     */
    pCollectionTuple
        .get(pushedBackForRetryAfterJobFailure)
        .apply(
            "Push Back BQ Load Request for Resubmission to RetryAfterJobFailureTopic post BQ Job Failure",
            PubsubIO.writeMessages()
                .to(options.getRetryAfterJobFailureTopic().get()));
    /*
     * Push Load Job Request to GCS dead letter location if we have exhausted set number of maxLoadRequestJobFailures
     * for a Load Job Request. A manual intervention/inspection/rectification is needed
     */
    pCollectionTuple.get(failedLoadJobRequests).apply(
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
            .discardingFiredPanes()).apply(
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
    pCollectionTuple.get(voidTupleTag).apply(new MonitorTrivialSink());

    p.run();
  }
}
