package com.example;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.coursera.metrics.datadog.DatadogReporter;
import org.coursera.metrics.datadog.transport.UdpTransport;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static org.coursera.metrics.datadog.DatadogReporter.Expansion.COUNT;
import static org.coursera.metrics.datadog.DatadogReporter.Expansion.MEAN;
import static org.coursera.metrics.datadog.DatadogReporter.Expansion.MEDIAN;
import static org.coursera.metrics.datadog.DatadogReporter.Expansion.P75;
import static org.coursera.metrics.datadog.DatadogReporter.Expansion.P95;
import static org.coursera.metrics.datadog.DatadogReporter.Expansion.P99;
import static org.coursera.metrics.datadog.DatadogReporter.Expansion.RATE_15_MINUTE;
import static org.coursera.metrics.datadog.DatadogReporter.Expansion.RATE_1_MINUTE;
import static org.coursera.metrics.datadog.DatadogReporter.Expansion.RATE_5_MINUTE;

public class SomeTimeConsumingDoFn extends DoFn<Long, Void> {

  private static final DatadogReporter.Builder datadogReporterBuilder =
      DatadogReporter.forRegistry(SharedMetricRegistries.getOrCreate("default"));
  private static final ConsoleReporter consoleReporter =
      ConsoleReporter.forRegistry(SharedMetricRegistries.getOrCreate("default")).build();
  private static final CustomMetricNameFormatter customMetricNameFormatter = new CustomMetricNameFormatter();
  private DatadogReporter datadogReporter;
  private UdpTransport udpTransport;
  private String project;
  private String appName;
  private String environment;
  private Boolean enableConsoleReporter;
  private String statsdHost;
  private Integer statsdPort;
  private String metricsPrefix;
  private String checkoutMetricName;
  private String orderedMetricName;
  private String shippedMetricName;
  private String deliveredMetricName;
  private String canceledMetricName;
  private String returnedMetricName;

  SomeTimeConsumingDoFn(Options options) {
    project = options.getMonitoredProjectId().get();
    appName = options.getAppName();
    environment = options.getEnvironment().get();
    statsdHost = options.getDataDogStatsdHost().get();
    statsdPort = options.getDataDogStatsdPort().get();
    metricsPrefix = options.getMetricsPrefix().get();

    /*
     * Enable Console Reporter(Needed for Debugging when using Direct Runner)
     */
    enableConsoleReporter = options.getEnableConsoleReporter().get();

    /*
     * Some Metrics that we are interested
     * We are using DogStatsD format where tags are extracted out
     * of Metric Name. For eg: env, appName, stage are tags below
     */
    checkoutMetricName =
        "transaction[env:" + environment + ",appName:" + appName + ",stage:checkout]";
    orderedMetricName =
        "transaction[env:" + environment + ",appName:" + appName + ",stage:ordered]";
    shippedMetricName =
        "transaction[env:" + environment + ",appName:" + appName + ",stage:shipped]";
    deliveredMetricName =
        "transaction[env:" + environment + ",appName:" + appName + ",stage:delivered]";
    canceledMetricName =
        "transaction[env:" + environment + ",appName:" + appName + ",stage:canceled]";
    returnedMetricName =
        "transaction[env:" + environment + ",appName:" + appName + ",stage:returned]";
  }

  @Setup
  public void doSetup() {
    /*
     * Initialize DataDog Reporter
     */
    datadogReporter =
        datadogReporterBuilder
            .withExpansions(
                EnumSet.of(COUNT, RATE_1_MINUTE, RATE_5_MINUTE, RATE_15_MINUTE, MEDIAN, MEAN, P75, P95, P99))
            .withHost(statsdHost)
            .withMetricNameFormatter(customMetricNameFormatter)
            .withTransport(
                new UdpTransport.Builder()
                    .withStatsdHost(statsdHost)
                    .withPort(statsdPort)
                    .withPrefix(metricsPrefix)
                    .build())
            .build();
    datadogReporter.start(5, TimeUnit.MINUTES);
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    /*
     * 1) Start Time Context
     * 2) Perform some long running operation
     * 3) Stop Time Context to capture the data points for the metric
     */

    /*
     * Capture Checkout Metrics
     */
    Timer checkoutTimer =
        SharedMetricRegistries.getOrCreate("default")
            .timer(MetricRegistry.name(checkoutMetricName));
    Transaction checkoutTransaction = new Transaction();
    try (Timer.Context checkoutContext = checkoutTimer.time()) {
      checkoutTransaction.execute();
      checkoutContext.stop();
    }

    /*
     *
     * Capture Ordered Metrics
     *
     */
    Timer orderedTimer =
        SharedMetricRegistries.getOrCreate("default").timer(MetricRegistry.name(orderedMetricName));

    Transaction orderedTransaction = new Transaction();
    try (Timer.Context orderedContext = orderedTimer.time()) {
      orderedTransaction.execute();
      orderedContext.stop();
    }

    /*
     *
     * Capture Shipped Metrics
     *
     */
    Timer shippedTimer =
        SharedMetricRegistries.getOrCreate("default").timer(MetricRegistry.name(shippedMetricName));

    Transaction shippedTransaction = new Transaction();
    try (Timer.Context shippedContext = shippedTimer.time()) {
      shippedTransaction.execute();
      shippedContext.stop();
    }

    /*
     *
     * Capture Delivered Metrics
     *
     */
    Timer deliveredTimer =
        SharedMetricRegistries.getOrCreate("default")
            .timer(MetricRegistry.name(deliveredMetricName));

    Transaction deliveredTransaction = new Transaction();
    try (Timer.Context deliveredContext = deliveredTimer.time()) {
      deliveredTransaction.execute();
      deliveredContext.stop();
    }

    /*
     *
     * Capture Canceled Metrics
     *
     */
    Timer canceledTimer =
        SharedMetricRegistries.getOrCreate("default")
            .timer(MetricRegistry.name(canceledMetricName));

    Transaction canceledTransaction = new Transaction();
    try (Timer.Context canceledContext = canceledTimer.time()) {
      canceledTransaction.execute();
      canceledContext.stop();
    }

    /*
     *
     * Capture Canceled Metrics
     *
     */
    Timer returnedTimer =
        SharedMetricRegistries.getOrCreate("default")
            .timer(MetricRegistry.name(returnedMetricName));

    Transaction returnedTransaction = new Transaction();
    try (Timer.Context returnedContext = returnedTimer.time()) {
      returnedTransaction.execute();
      returnedContext.stop();
    }
    /*
     * Ship Metrics to Datadog via UDP to DogStatsD Server
     * This won't block
     */
    datadogReporter.report();
    if (enableConsoleReporter) {
      consoleReporter.report();
    }
  }

  @Teardown
  public void doTearDown() {
    consoleReporter.stop();
    datadogReporter.stop();
  }
}
