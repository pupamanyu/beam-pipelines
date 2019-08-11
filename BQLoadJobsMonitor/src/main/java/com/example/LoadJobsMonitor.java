package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class LoadJobsMonitor {

  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    final TupleTag<String> pushedBackForMonitoring = new TupleTag<String>();
    final TupleTag<Void> voidTupleTag = new TupleTag<Void>();

    PCollectionTuple pCollectionTuple =
        p.apply(
                "Consume BQ Load Jobs to be Monitored",
                PubsubIO.readStrings()
                    .fromSubscription(options.getSourceSubscription())
                    .withIdAttribute(options.getDeDupId().get()))
            .apply(
                "Monitor BQ Load Jobs",
                ParDo.of(new MonitorJobDoFn(options, pushedBackForMonitoring))
                    .withOutputTags(voidTupleTag, TupleTagList.of(pushedBackForMonitoring)));

    /*
     * Resubmit Jobs for Monitoring post Back Off Exhaustion
     */
    pCollectionTuple
        .get(pushedBackForMonitoring)
        .apply(
            "Push Back BQ Load Jobs for Monitoring",
            PubsubIO.writeStrings().to(options.getSourceTopic().get()).withIdAttribute(options.getDeDupId().get()));
    pCollectionTuple.get(voidTupleTag).apply(new MonitorTrivialSink());
  }
}
