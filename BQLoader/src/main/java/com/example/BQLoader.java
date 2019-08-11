package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;

public class BQLoader {

  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    final TupleTag<KV<String, String>> submittedLoadJobs = new TupleTag<KV<String, String>>();
    final TupleTag<String> submittedForRetryLoadJobs = new TupleTag<String>();
    final TupleTag<Void> voidTupleTag = new TupleTag<Void>();

    PCollectionTuple pCollectionTuple =
        p.apply(
                "Consume Load Requests",
                PubsubIO.readStrings().fromSubscription(options.getSourceSubscription()))
            .apply(
                "Submit BQ Load Jobs",
                ParDo.of(new BQLoaderDoFn(options, submittedLoadJobs, submittedForRetryLoadJobs))
                    .withOutputTags(
                        voidTupleTag,
                        TupleTagList.of(submittedLoadJobs).and(submittedForRetryLoadJobs)));

    /*
     * Extract Successfully Submitted Job State Data
     * TODO: Need to Implement Sink for State DB(BigTable or another Key Value Store)
     *  pCollectionTuple.get(submittedLoadJobs) --> PCollection<KV<String, String>>
     *  where K --> JobId, V --> LoadRequest JSON String
     */

    /*
     * Extract JobId from PCollection<KV<String, String>> for
     * feeding it to Job Monitoring PubSub Topic
     */
    pCollectionTuple
        .get(submittedLoadJobs)
        .apply(MapElements.into(TypeDescriptors.strings()).via(KV::getKey))
        .apply(
            "Feed the Submitted Load Jobs for Monitoring Queue",
            PubsubIO.writeStrings().to(options.getJobMonitoringTopic().get()));

    /*
     * Extract The Jobs for Retry Submission to the Source PubSub Topic
     */
    pCollectionTuple
        .get(submittedForRetryLoadJobs)
        .apply(
            "Inject Load Requests to Source Queue",
            PubsubIO.writeStrings().to(options.getSourceTopic().get()));

    /*
     * Trivial Sink for Main Output(PCollection<Void>)
     */
    pCollectionTuple.get(voidTupleTag).apply("Trivial Sink", new LoaderTrivialSink());

    p.run();
  }
}
