package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Shuffle {
  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);
    List<String> list = new ArrayList<String>(Arrays.asList("Apple", "Bat", "Cat"));
    PCollection<String> pCollection =
        p.apply(Create.of(list)).apply("Shuffle", ShuffleTransform.of());
    p.run();
  }
}
