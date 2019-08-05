package com.example;

import org.apache.beam.sdk.transforms.DoFn;

public class BundleToString extends DoFn<Bundle, String> {

  @ProcessElement
  public void processElement(ProcessContext context) {
    context.output(context.element().toString());
  }
}
