/*  Copyright 2019 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.example;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.RandomStringUtils;

public class ShuffleTransform<T> extends PTransform<PCollection<T>, PCollection<T>> {

  private ShuffleTransform() {};

  public static <T> ShuffleTransform<T> of() {
    return new ShuffleTransform<T>();
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
  public PCollection<T> expand(PCollection<T> input) {
    return input
        .apply(
            "Add Random Key to the elements",
            ParDo.of(new ReKey<T>(input.getPipeline().getOptions().as(Options.class))))
        .apply("Group by Random keys", GroupByKey.<String, T>create())
        .apply("Remove Random Keys, and Extract Values", Values.<Iterable<T>>create())
        .apply("Flatten", Flatten.<T>iterables());
  }

  private static class ReKey<T> extends DoFn<T, KV<String, T>> {
    private final ValueProvider<Integer> keyLength;

    ReKey(Options options) {
      this.keyLength = options.getKeyLength();
    }

    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<KV<String, T>> outputReceiver) {
      outputReceiver.output(
          KV.of(RandomStringUtils.randomAlphanumeric(this.keyLength.get()), element));
    }
  }
}
