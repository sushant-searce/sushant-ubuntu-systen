package com.striiv.dataflow;

import com.striiv.dataflow.transforms.GenericFileArchiveTransforms;
//import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * Created by herbertlee on 6/18/18.
 * This pipeline takes files from Pub/Sub and archives them to Google Cloud Storage.
 */
public class GenericFileArchivePipeline {

    public static void main(String[] args) {
    	GenericFileArchivePipeline.GenericFileArchiveDataPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(GenericFileArchivePipeline.GenericFileArchiveDataPipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadUnparsedPostsFromPubSub", PubsubIO.readStrings()
                        .fromTopic(options.getInputTopic().get()))
                .apply("WindowAndFormatPosts", new GenericFileArchivePTransform())
                .apply("WriteArchiveToGCS", ParDo.of(new GenericFileArchiveTransforms.WriteArchiveToGCSFn(options.getBucketId().get())));

        pipeline.run();
    }

    static class GenericFileArchivePTransform extends PTransform<PCollection<String>, PCollection<String>> {

        @Override
        public PCollection<String> expand(PCollection<String> unparsedPosts) {
            return unparsedPosts.apply("WindowByMinute", Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                    .apply("FormatPostsWithTimestamp", ParDo.of(new GenericFileArchiveTransforms.FormatPostWithTimestamp()))
                    .apply("ConcatenatePosts", Combine.globally(
                            new GenericFileArchiveTransforms.ConcatenatePostsFn()).withoutDefaults());
        }
    }
    
    public interface GenericFileArchiveDataPipelineOptions extends FlinkPipelineOptions {
        @Description("Pubsub topic that the pipeline will read from.  Required if the pipeline is being run in " +
                "streaming mode.  Must be a fully-qualified pubsub topic path like " +
                "`projects/project-id/topics/topic-name`.")
        ValueProvider<String> getInputTopic();
        void setInputTopic(ValueProvider<String> topic);

        @Description("Bucket that the pipeline will store data to.")
        ValueProvider<String> getBucketId();
        void setBucketId(ValueProvider<String> bucketId);
    }
}