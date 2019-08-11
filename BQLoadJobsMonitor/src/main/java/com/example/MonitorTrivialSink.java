package com.example;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class MonitorTrivialSink extends PTransform<PCollection<Void>, PDone> {
  @Override
  public PDone expand(PCollection<Void> input) {
    return PDone.in(input.getPipeline());
  }
}
