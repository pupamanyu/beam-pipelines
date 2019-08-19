package com.example;

import org.coursera.metrics.datadog.DefaultMetricNameFormatter;

public class CustomMetricNameFormatter extends DefaultMetricNameFormatter {
  @Override
  public String format(String name, String... path) {
    // Make response metrics names less verbose
    String newName = name.replace("io.dropwizard.jetty.MutableServletContextHandler", "");

    // Call DefaultMetricNameFormatter to handle tags
    return super.format(newName, path);
  }
}
