package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

public class BeamPipelineMetrics {

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);
    p.apply(GenerateSequence.from(0))
        .apply(
            Window.<Long>into(FixedWindows.of(Duration.standardMinutes(1)))
                .triggering(AfterWatermark.pastEndOfWindow())
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes())
        .apply(
            "Trivial Metrics Sink",
            new TrivialMetricsSink(options));
    p.run();
  }
}
