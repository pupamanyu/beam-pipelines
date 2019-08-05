package com.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BundleFilesBySize extends DoFn<KV<String, String>, Bundle> {

  private static final Logger LOG = LoggerFactory.getLogger(BundleFilesBySize.class);

  @StateId("batchBuffer")
  private final StateSpec<BagState<FileNotification>> batchBuffer = StateSpecs.bag();

  @StateId("batchSize")
  private final StateSpec<ValueState<Long>> batchSize = StateSpecs.value(VarLongCoder.of());

  @StateId("fileCount")
  private final StateSpec<ValueState<Integer>> fileCount = StateSpecs.value(VarIntCoder.of());

  @TimerId("expiry")
  private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @TimerId("bufferStale")
  private final TimerSpec staleSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  private long MAX_BUNDLE_BYTES;
  private int BUNDLE_ELEMENTS_THRESHOLD;
  private Duration STALENESS_MINUTES;
  private Duration LATENESS_MINUTES;
  private ObjectMapper objectMapper;


  BundleFilesBySize(Options options) {
    // Maximum Total Size of the file bundle, this needs to be set per data type or adjusted
    // if file sizes vary
    MAX_BUNDLE_BYTES = options.getBundleSizeBytes().get();
    // Need to Adjust STALENESS_MINUTES
    STALENESS_MINUTES = Duration.standardMinutes(options.getStalenessMinutes().get());
    LATENESS_MINUTES = Duration.standardMinutes(options.getAllowedLatenessMinutes().get());
    BUNDLE_ELEMENTS_THRESHOLD = options.getBundleElementsThreshold().get();
  }

  private void flush(
      WindowedContext context,
      BagState<FileNotification> batchBufferState,
      ValueState<Long> batchSizeState,
      ValueState<Integer> fileCountState) {

    Bundle bundle = new Bundle();

    for (FileNotification fileNotification: batchBufferState.read()) {
      bundle.add(
          "gs://" + fileNotification.bucket() + "/" + fileNotification.name(),
          fileNotification.size());
    }

    bundle.setBundlePrefix();

    LOG.info(bundle.toString());

    /*
     * Output Bundle
     */
    context.output(bundle);

    /*
     * Clear State
     */
    batchBufferState.clear();
    batchSizeState.clear();
    fileCountState.clear();
  }

  @Setup
  public void doSetup() {
    this.objectMapper = new ObjectMapper();
  }

  @ProcessElement
  public void processElement(
      ProcessContext context,
      BoundedWindow window,
      PaneInfo paneInfo,
      @StateId("batchBuffer") BagState<FileNotification> batchBufferState,
      @StateId("batchSize") ValueState<Long> batchSizeState,
      @StateId("fileCount") ValueState<Integer> fileCountState,
      @TimerId("bufferStale") Timer staleTimer,
      @TimerId("expiry") Timer expiryTimer) {
    if (MoreObjects.firstNonNull(batchSizeState.read(), 0L) == 0L) {
      /*
       * We set Expiry Timer(Event Time) and Stale Timer(Processing Time)
       * only when we see an Element and Buffer is empty
       */
      LOG.debug(
          "Buffer is Empty for the Window {}. Setting Stale and Expiry Timers", window.toString());
      /*
       * Set the Stale Timer so we can flush the stale Buffer.
       * STALENESS_MINUTES < Min(window.maxTimestamp(), this.LATENESS_MINUTES)
       */
      staleTimer.align(this.STALENESS_MINUTES).setRelative();
      /*
       * Set Expiry Timer so that we can flush pending Buffer when timer expires
       * Set Expiry until LATENESS_MINUTES for the Late Pane Elements
       * Set Expiry until Window Max TimeStamp for the Early or OnTime Pane Elements
       */
      if (paneInfo.getTiming().equals(PaneInfo.Timing.LATE)) {
        expiryTimer.set(window.maxTimestamp().plus(this.LATENESS_MINUTES));
      } else {
        expiryTimer.set(window.maxTimestamp());
      }
    }

    try {
      FileNotification fileNotification =
          this.objectMapper.readValue(context.element().getValue(), FileNotification.class);
      batchBufferState.add(fileNotification);
      LOG.info(
          "Added element with timestamp {} to the Window {}",
          context.timestamp(),
          window.toString());

      int accumulatedElementCount = MoreObjects.firstNonNull(fileCountState.read(), 0) + 1;
      fileCountState.write(accumulatedElementCount);

      long accumulatedBundleSize =
          MoreObjects.firstNonNull(batchSizeState.read(), 0L) + fileNotification.size();
      batchSizeState.write(accumulatedBundleSize);

      /*
       * If Bundle has more than BUNDLE_ELEMENTS_THRESHOLD Elements, then Flush Buffer
       */
      if (accumulatedElementCount >= this.BUNDLE_ELEMENTS_THRESHOLD) {
        LOG.info(
            "Bundle Count reached the threshold of {} Elements for the Window {}",
            this.BUNDLE_ELEMENTS_THRESHOLD,
            window.toString());
        flush(context, batchBufferState, batchSizeState, fileCountState);
      }
      /*
       * If Total Bundle Size is more than MAX_BUNDLE_BYTES, then Flush Buffer
       */
      if (accumulatedBundleSize >= this.MAX_BUNDLE_BYTES) {
        LOG.info(
            "Bundle Size reached the threshold of {} bytes  for the Window {}",
            this.MAX_BUNDLE_BYTES,
            window.toString());
        flush(context, batchBufferState, batchSizeState, fileCountState);
      }
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
  }

  @OnTimer("bufferStale")
  public void onStale(
      OnTimerContext context,
      @StateId("batchBuffer") BagState<FileNotification> batchBufferState,
      @StateId("batchSize") ValueState<Long> batchSizeState,
      @StateId("fileCount") ValueState<Integer> batchCountState) {
    if (!MoreObjects.firstNonNull(batchBufferState.isEmpty().read(), true)) {
      flush(context, batchBufferState, batchSizeState, batchCountState);
      LOG.info(
          "Stale Timer Triggered at {} for the Window {}",
          context.timestamp(),
          context.window().toString());
    }
  }

  @OnTimer("expiry")
  public void onExpiry(
      OnTimerContext context,
      @StateId("batchBuffer") BagState<FileNotification> batchBufferState,
      @StateId("batchSize") ValueState<Long> batchSizeState,
      @StateId("fileCount") ValueState<Integer> fileCountState) {
    if (!MoreObjects.firstNonNull(batchBufferState.isEmpty().read(), true)) {
      flush(context, batchBufferState, batchSizeState, fileCountState);
      LOG.info(
          "Expiry Timer Triggered at {} for the Window {}",
          context.timestamp(),
          context.window().toString());
    }
  }
}
