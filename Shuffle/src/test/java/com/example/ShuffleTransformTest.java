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

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(JUnit4.class)
public class ShuffleTransformTest {
  private final transient Options options = PipelineOptionsFactory.create().as(Options.class);
  @Rule public final transient TestPipeline p = TestPipeline.fromOptions(options);
  @Rule public ExpectedException thrown = ExpectedException.none();


  @Test
  public  void testShuffleTransformString() {
    List<String> list = new ArrayList<String>(Arrays.asList("Apple", "Bat", "Cat"));
    PCollection<String> testOutput = p.apply(Create.of(list)).apply("Shuffle", ShuffleTransform.of());
    PAssert.that(testOutput).containsInAnyOrder(list);
    p.run();
  }

  @Test
  public  void testShuffleTransformInteger() {
    List<Integer> list = new ArrayList<Integer>(Arrays.asList(1, 2, 3));
    PCollection<Integer> testOutput = p.apply(Create.of(list)).apply("Shuffle", ShuffleTransform.of());
    PAssert.that(testOutput).containsInAnyOrder(list);
    p.run();
  }

  @Test
  public  void testShuffleTransformEmpty() {
    PCollection<String> testOutput = p.apply(Create.empty(StringUtf8Coder.of())).apply("Shuffle", ShuffleTransform.of());
    PAssert.that(testOutput).empty();
    p.run();
  }
}
