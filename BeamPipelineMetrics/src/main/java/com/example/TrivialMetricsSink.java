package com.example;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class TrivialMetricsSink extends PTransform<PCollection<Long>, PDone> {

  private Options options;

  TrivialMetricsSink(Options options) {
    this.options = options;
  }
  /**
   * Override this method to specify how this {@code PTransform} should be expanded on the given
   * {@code InputT}.
   *
   * <p>NOTE: This method should not be called directly. Instead apply the {@code PTransform} should
   * be applied to the {@code InputT} using the {@code apply} method.
   *
   * <p>Composite transforms, which are defined in terms of other transforms, should return the
   * output of one of the composed transforms. Non-composite transforms, which do not apply any
   * transforms internally, should return a new unbound output and register evaluators (via
   * backend-specific registration methods).
   *
   * @param input
   */
  @Override
  public PDone expand(PCollection<Long> input) {
    input.apply(
        "Perform some operations and push operational metrics via DogStatsD(UDP)",
        ParDo.of(new TimeConsumingDoFn(options)));
    return PDone.in(input.getPipeline());
  }
}
