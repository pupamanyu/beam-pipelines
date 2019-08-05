/*
#
# Copyright (C) 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
*/

package com.example;

import com.google.common.base.Preconditions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutablePeriod;
import org.joda.time.format.PeriodFormatterBuilder;
import org.joda.time.format.PeriodParser;

import java.util.Locale;

public class KafkaCommit {
  /**
   * Options supported by {@link KafkaToGCS}.
   */
  public interface Options extends PipelineOptions, StreamingOptions, DataflowPipelineOptions {

    @Description("Output Directory to write the output to. Must end with /")
    @Validation.Required
    ValueProvider<String> getOutputDirectory();
    void setOutputDirectory(ValueProvider<String> value);

    @Description("Kafka topic to read the input from")
    @Validation.Required
    ValueProvider<String> getTopic();
    void setTopic(ValueProvider<String> value);

    @Description("Kafka BootStrap Servers. For example: myServer1:9092,myServer2:9092")
    @Validation.Required
    ValueProvider<String> getBootstrapServers();
    void setBootstrapServers(ValueProvider<String> value);

    @Description("The window duration in which data will be written. Defaults to 5m. "
        + "Allowed formats are: "
        + "Ns (for seconds, example: 5s), "
        + "Nm (for minutes, example: 12m), "
        + "Nh (for hours, example: 2h).")
    @Default.String("1h")
    ValueProvider<String> getWindowDuration();
    void setWindowDuration(ValueProvider<String> value);

    @Description("GCS Output Filename Prefix")
    @Default.String("output")
    ValueProvider<String> getOutputPrefix();
    void setOutputPrefix(ValueProvider<String> value);

    @Description("GCS Output Filename Suffix")
    @Default.String(".txt")
    ValueProvider<String> getOutputSuffix();
    void setOutputSuffix(ValueProvider<String> value);

    @Description("Number of Shards")
    @Default.Integer(1)
    ValueProvider<Integer> getNumShards();
    void setNumShards(ValueProvider<Integer> value);

  }

  private static class ExtractKafkaRecordValues
      extends PTransform<PCollection<KafkaRecord<String, String>>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<KafkaRecord<String, String>> kafkaRecordPCollection) {
      return kafkaRecordPCollection.apply("KafkaRecordToString", MapElements.via(
          new SimpleFunction<KafkaRecord<String, String>, String>() {

            @Override public String apply(KafkaRecord<String, String> input) {
              return input.getKV().getValue();
            }
          }));
    }
  }

  /**
   * Parses a duration from a period formatted string. Values
   * are accepted in the following formats:
   * <p>
   * Ns - Seconds. Example: 5s<br>
   * Nm - Minutes. Example: 13m<br>
   * Nh - Hours. Example: 2h
   *
   * <pre>
   * parseDuration(null) = NullPointerException()
   * parseDuration("")   = Duration.standardSeconds(0)
   * parseDuration("2s") = Duration.standardSeconds(2)
   * parseDuration("5m") = Duration.standardMinutes(5)
   * parseDuration("3h") = Duration.standardHours(3)
   * </pre>
   *
   * @param value The period value to parse.
   * @return  The {@link Duration} parsed from the supplied period string.
   */
  private static Duration parseDuration(String value) {
    Preconditions.checkNotNull(value, "The specified duration must be a non-null value!");

    PeriodParser parser = new PeriodFormatterBuilder()
        .appendSeconds().appendSuffix("s")
        .appendMinutes().appendSuffix("m")
        .appendHours().appendSuffix("h")
        .toParser();

    MutablePeriod period = new MutablePeriod();
    parser.parseInto(period, value, 0, Locale.getDefault());

    return period.toDurationFrom(new DateTime(0));
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("Read Kafka Messages",
            KafkaIO
                .<String, String>read()
                .withBootstrapServers(options.getBootstrapServers().get())
                .withTopic(options.getTopic().get())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withReadCommitted()
                .withoutMetadata()
                .updateConsumerProperties(ImmutableMap.of("enable.auto.commit", "false"))
                .commitOffsetsInFinalize())
        .apply("Extract Kafka Record Values", new ExtractKafkaRecordValues())
        .apply(options.getWindowDuration().get() + " Fixed Windows",
            Window.<String>into(FixedWindows.of(parseDuration(options.getWindowDuration().get())))
                .triggering(AfterProcessingTime
                    .pastFirstElementInPane()
                    .plusDelayOf(parseDuration(options.getWindowDuration().get())))
                .discardingFiredPanes())
        .apply("Write to GCS",
            FileIO.<String>write()
                .to(options.getOutputDirectory().get())
                .withPrefix(options.getOutputPrefix().get())
                .withSuffix(options.getOutputSuffix().get())
                .withNumShards(options.getNumShards().get()));
    }
}