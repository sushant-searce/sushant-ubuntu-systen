package com.striiv.dataflow;

import com.google.gson.JsonObject;
import com.striiv.dataflow.adapters.algoparams.ConfigurableParamsServiceWebAdapter;
import com.striiv.dataflow.coders.JsonObjectCoder;
import com.striiv.dataflow.transforms.DataPointTransforms;
import com.striiv.dataflow.transforms.DatastoreTransforms;
import com.striiv.dataflow.trigger_pipelines.Keys;
import com.striiv.dataflow.trigger_pipelines.TriggerPipelineOptions;
import com.striiv.proto.binary.UnifiedBinary;
import com.striiv.proto.core.Timeseries;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


public class StepAndGaitPipeline {
    private static final long NOTIFICATION_FREQUENCY = 60 * 60L;
    private static final String NAMESPACE = "StepAndGait";

    public static void main(String[] args) {
        TriggerPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(TriggerPipelineOptions.class);

        var pipeline = Pipeline.create(options);
        pipeline.getCoderRegistry().registerCoderForClass(JsonObject.class, JsonObjectCoder.of());

        var messages = pipeline.
                apply("ReadUnifiedBinaryMessagesFromPubSub", PubsubIO.readProtos(UnifiedBinary.UnifiedBinaryMessage.class)
                        .fromTopic(options.getInputTopic()))
                .apply("SAG Flow Transforms", new SAGFlowTransform(options));

        if (options.getOutputTopic().isAccessible())
            messages.apply("Write to Pubsub", PubsubIO.writeMessages().to(options.getOutputTopic().get()));

        pipeline.run();
    }

    public static class SAGFlowTransform
            extends PTransform<PCollection<UnifiedBinary.UnifiedBinaryMessage>, PCollection<PubsubMessage>> {
        private final transient TriggerPipelineOptions options;

        public SAGFlowTransform(TriggerPipelineOptions options) {
            this.options = options;
        }

        @Override
        public PCollection<PubsubMessage> expand(PCollection<UnifiedBinary.UnifiedBinaryMessage> input) {
            return input
                    .apply("Filter SAG Unified Binary Message", ParDo.of(new FilterSAGUnifiedBinaryMessage()))
                    .apply("Extract local types", new InputExtractor(options))
                    .apply("Apply 60 Minutes Fixed Window",
                            Window.<Keys>into(
                                    FixedWindows.of(Duration.standardMinutes(60)))
                                    .withAllowedLateness(Duration.ZERO))
                    .apply("Group By User Id Device Id Rounded Timestamp", Distinct.create())
                    .apply("Remove repeated notifications", Distinct.create())
                    .apply("Convert to Pubsub", ParDo.of(new PubsubNotificator()));
        }
    }

    public static class PubsubNotificator extends DoFn<Keys, PubsubMessage> {
        private static final Counter counter = Metrics.counter(NAMESPACE, "requests");

        @ProcessElement
        public void convert(ProcessContext ctx) {
            Keys k = Objects.requireNonNull(ctx.element());
            Objects.requireNonNull(k);

            Map<String, String> attributes = new HashMap<>();
            attributes.put("user_id", String.valueOf(k.getUserId()));
            attributes.put("device_id", k.getDeviceId());
            attributes.put("timestamp", String.valueOf(k.getTimestamp()));
            attributes.put("level", "hourly");

            var empty = new byte[]{};
            ctx.output(new PubsubMessage(empty, attributes));
            counter.inc();
        }
    }

    public static class InputExtractor extends
            PTransform<PCollection<UnifiedBinary.UnifiedBinaryMessage>, PCollection<Keys>> {
        private final transient TriggerPipelineOptions options;

        public InputExtractor(TriggerPipelineOptions options) {
            this.options = options;
        }

        @Override
        public PCollection<Keys> expand(PCollection<UnifiedBinary.UnifiedBinaryMessage> input) {
            var paramsServiceWebAdapter = new ConfigurableParamsServiceWebAdapter(
                    options.getConfigurableParamsServiceHost().get(),
                    options.getConfigurableParamsServiceApiKey().get());

            return input.apply("Time drift correction", ParDo.of(
                    new DataPointTransforms.UnifiedBinaryMessageToDataPointUnifiedBinaryEntryKV(paramsServiceWebAdapter)))
                    .apply("Retain Key", ParDo.of(retainKey()));
        }

        static DoFn<KV<JsonObject, KV<Timeseries.DataPoint, UnifiedBinary.UnifiedBinaryEntry>>, Keys>
        retainKey() {
            return new DoFn<KV<JsonObject, KV<Timeseries.DataPoint, UnifiedBinary.UnifiedBinaryEntry>>, Keys>() {
                @ProcessElement
                public void convert(ProcessContext context) {
                    var kvPoint = Objects.requireNonNull(context.element()).getValue();
                    Objects.requireNonNull(kvPoint);

                    var dataPoint = kvPoint.getKey();
                    Objects.requireNonNull(dataPoint);

                    long msPeriod = NOTIFICATION_FREQUENCY * 1000L;
                    long timestamp = dataPoint.getTimestamp();

                    var k = new Keys(
                            Long.parseLong(dataPoint.getUserId()),
                            dataPoint.getDeviceId(),
                            timestamp - (timestamp % msPeriod)
                    );

                    context.output(k);
                }
            };
        }
    }

    static class FilterSAGUnifiedBinaryMessage extends DatastoreTransforms.AbstractDatastoreFn
            <UnifiedBinary.UnifiedBinaryMessage, UnifiedBinary.UnifiedBinaryMessage> {

        private final Set<Integer> expectedStepMembers = Set.of(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.STEP_COUNT.getNumber()
        );

        @ProcessElement
        public void processElement(ProcessContext processContext) {
            UnifiedBinary.UnifiedBinaryMessage message = processContext.element();

            boolean isMessageSAG = Objects.requireNonNull(message).getEntryList().stream()
                    .anyMatch(e -> expectedStepMembers.contains(e.getEntryDataCase().getNumber()));

            if (isMessageSAG)
                processContext.output(message);
        }
    }
}