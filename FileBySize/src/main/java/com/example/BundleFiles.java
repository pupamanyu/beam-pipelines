package com.example;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

public class BundleFiles {

  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    p.apply(
            "Read GCS Notifications",
            PubsubIO.readStrings().fromSubscription(options.getSubscription()))
        .apply(
            "Fixed Window",
            Window.<String>into(
                    FixedWindows.of(
                        Duration.standardMinutes(options.getWindowDurationMinutes().get())))
                .triggering(
                    AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(
                            AfterPane.elementCountAtLeast(options.getEarlyFiringElementsThreshold().get()))
                        .withLateFirings(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(
                                    Duration.standardMinutes(
                                        options.getAllowedLatenessMinutes().get()))))
                .withAllowedLateness(
                    Duration.standardMinutes(options.getAllowedLatenessMinutes().get()),
                    Window.ClosingBehavior.FIRE_ALWAYS)
                .withTimestampCombiner(TimestampCombiner.EARLIEST)
                .discardingFiredPanes())
        .apply("Add Keys", ParDo.of(new ReKey(options)))
        .apply("Bundle Files", ParDo.of(new BundleFilesBySize(options)))
        .apply("Bundles to String", ParDo.of(new BundleToString()))
        .apply(
            "Write Bundles to File",
            TextIO.write()
                .withNumShards(1)
                .withWindowedWrites()
                .to(options.getOutputPrefix().get()));
    p.run();
  }
}
