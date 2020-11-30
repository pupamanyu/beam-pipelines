/*
 * Copyright (c) 2020 Google Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

public class CacheState {

  public static void main(String[] args) {
    // Set Options from command line arguments
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    String FilePrefix = options.getGcsBucket().get() + "/" + options.getFileNamePrefix().get();
    // Initialize Pipeline
    Pipeline p = Pipeline.create(options);
    p.apply(GenerateSequence.from(0).withRate(1L, Duration.millis(1L))).apply(
        Window.<Long>into(FixedWindows.of(Duration.standardSeconds(60L))).triggering(Repeatedly
            .forever(AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(Duration.standardSeconds(60L)))).withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes()).apply("Add Random Keys", ParDo.of(new ReKeyDoFn()))
        .apply("Prototype DoFn for Caching State Externally", ParDo
            .of(new CacheStateDoFn(options.getProject(), options.getTableInstance().get(),
                options.getTableName().get(), options.getColumnFamily().get()))).apply("Write to GCS Sink",
        TextIO.write().to(FilePrefix).withWindowedWrites()
            .withNumShards(options.getNumberOfShards().get()));
    ;
    p.run();
  }
}
