package com.example;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface Options extends PipelineOptions, DataflowPipelineOptions, StreamingOptions {

  @Description("Monitored GCP Project ID")
  @Validation.Required
  ValueProvider<String> getMonitoredProjectId();

  void setMonitoredProjectId(ValueProvider<String> value);

  @Description("Environment for the Metric: For example, one of development, staging, production")
  @Validation.Required
  ValueProvider<String> getEnvironment();

  void setEnvironment(ValueProvider<String> value);

  @Description("DataDogStatsD Server Hostname(FQDN)")
  @Validation.Required
  ValueProvider<String> getDataDogStatsdHost();

  void setDataDogStatsdHost(ValueProvider<String> value);

  @Description("DataDogStatsD Server Port(FQDN)")
  @Validation.Required
  ValueProvider<Integer> getDataDogStatsdPort();

  void setDataDogStatsdPort(ValueProvider<Integer> value);

  @Description("Common Metrics Prefix")
  @Default.String("")
  ValueProvider<String> getMetricsPrefix();

  void setMetricsPrefix(ValueProvider<String> value);

  @Description("Enable Console Reporter")
  @Default.Boolean(false)
  ValueProvider<Boolean> getEnableConsoleReporter();

  void setEnableConsoleReporter(ValueProvider<Boolean> value);

  @Description("Pipeline Stage Name to be used for the Latency Metric")
  @Validation.Required
  ValueProvider<String> getPipelineStageName();

  void setPipelineStageName(ValueProvider<String> value);
}
