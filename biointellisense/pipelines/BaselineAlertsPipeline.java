package com.striiv.dataflow;

import com.google.gson.JsonObject;
import com.striiv.dataflow.adapters.algoparams.ConfigurableParamsServiceWebAdapter;
import com.striiv.dataflow.coders.JsonObjectCoder;
import com.striiv.dataflow.schemas.KeyPoint;
import com.striiv.dataflow.transforms.DataPointTransforms;
import com.striiv.dataflow.utils.TimestampUtils;
import com.striiv.proto.binary.Temperature;
import com.striiv.proto.binary.UnifiedBinary;
import com.striiv.proto.core.Timeseries;
//import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.util.*;

import static com.striiv.dataflow.utils.TimestampUtils.MILLISECONDS_IN_MINUTE;

public class BaselineAlertsPipeline {

    public interface Options extends FlinkPipelineOptions {
        @Description("Pubsub topic that the pipeline will read from.  Required if the pipeline is being run in " +
                "streaming mode.  Must be a fully-qualified pubsub topic path like " +
                "`projects/project-id/topics/topic-name`.")
        ValueProvider<String> getInputTopic();
        void setInputTopic(ValueProvider<String> inputTopic);

        @Description("Pubsub topic that the pipeline will write to.  Required if the pipeline is being run in " +
                "streaming mode.  Must be a fully-qualified pubsub topic path like " +
                "`projects/project-id/topics/topic-name`.")
        ValueProvider<String> getOutputTopic();
        void setOutputTopic(ValueProvider<String> outputTopic);

        @Description("Host URL of the Configurable Parameters service this pipeline should call.")
        @Validation.Required
        ValueProvider<String> getConfigurableParamsServiceHost();
        void setConfigurableParamsServiceHost(ValueProvider<String> configurableParamsServiceHost);

        @Description("API Key of the Configurable Parameters service this pipeline should call.")
        @Validation.Required
        ValueProvider<String> getConfigurableParamsServiceApiKey();
        void setConfigurableParamsServiceApiKey(ValueProvider<String> configurableParamsServiceApiKey);
    }

    private enum Inputs {
        SKIN_TEMP,
        AMBIENT_TEMP,
        HEART_RATE,
        HR_CONFIDENCE,
        RESP_RATE,
        RR_CONFIDENCE,
        AC_MOTION
    }

    private static final int ALERT_FREQUENCY = 3600; // 1 hour

    public static void main(String[] args) {
        var options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(BaselineAlertsPipeline.Options.class);

        var pipeline = Pipeline.create(options);
        pipeline.getCoderRegistry().registerCoderForClass(JsonObject.class, JsonObjectCoder.of());

        var configParams = new ConfigurableParamsServiceWebAdapter(
                options.getConfigurableParamsServiceHost().get(), options.getConfigurableParamsServiceApiKey().get());

        var messages = pipeline.apply("ReadUnifiedBinaryMessagesFromPubSub",
                PubsubIO.readProtos(UnifiedBinary.UnifiedBinaryMessage.class).fromTopic(options.getInputTopic()))
                .apply(new ProcessInputs(configParams, BaselineAlertsPipeline.ALERT_FREQUENCY))
                .apply("MakePubsubMessage", ParDo.of(new PubsubNotificator()));

        if (options.getOutputTopic().isAccessible())
            messages.apply("WriteToPubsub", PubsubIO.writeMessages().to(options.getOutputTopic().get()));

        pipeline.run();
    }

   public static class ProcessInputs
            extends PTransform<PCollection<UnifiedBinary.UnifiedBinaryMessage>, PCollection<KeyPoint>> {
        private final int notificationFrequency;
        private final ConfigurableParamsServiceWebAdapter configParams;

        public ProcessInputs(ConfigurableParamsServiceWebAdapter configParams, int notificationFrequency) {
            this.notificationFrequency = notificationFrequency;
            this.configParams = configParams;
        }

        @Override
        public PCollection<KeyPoint> expand(PCollection<UnifiedBinary.UnifiedBinaryMessage> input) {
            return input
                    .apply("ExtractLocalTypes", new BaselineAlertsPipeline.InputExtractor(configParams))
                    .apply("ApplyFixedWindow", Window.into(FixedWindows.of(Duration.standardSeconds(notificationFrequency))))
                    .apply("GroupByUserAndTimestamp", Combine.perKey(new InputMerger()))
                    .apply("EmitDatapointsWithCompleteInputs", ParDo.of(new InputCheckerFn()))
                    .apply("RoundTimestampDown", ParDo.of(new RoundTimestampsToHour()))
                    .apply("GroupSameEvents", Distinct.create());
        }
    }

    private static class RoundTimestampsToHour extends DoFn<KeyPoint, KeyPoint> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            var keyPoint = context.element();
            context.output(KeyPoint.of(keyPoint.getUserId(),
                    keyPoint.getDeviceId(),
                    TimestampUtils.roundTimestampDown(keyPoint.getTimestamp(),60 * MILLISECONDS_IN_MINUTE),
                    keyPoint.getAccelDutyCycle()));
        }
    }

    private static class InputExtractor extends
            PTransform<PCollection<UnifiedBinary.UnifiedBinaryMessage>, PCollection<KV<KeyPoint, Set<Inputs>>>> {
        private static final int SKIN_LOCATION = Temperature.TemperatureLocation.TemperatureLocation_Skin_VALUE;
        private static final int AMBIENT_LOCATION = Temperature.TemperatureLocation.TemperatureLocation_Ambient_VALUE;
        private ConfigurableParamsServiceWebAdapter configurableParamsService;

        public InputExtractor(ConfigurableParamsServiceWebAdapter configurableParamsService) {
            this.configurableParamsService = configurableParamsService;
        }

        @Override
        public PCollection<KV<KeyPoint, Set<Inputs>>> expand(PCollection<UnifiedBinary.UnifiedBinaryMessage> input) {
            return input
                    .apply("TimeDriftCorrection", ParDo.of(new DataPointTransforms.UnifiedBinaryMessageToDataPointUnifiedBinaryEntryKV(configurableParamsService)))
                    .apply("ConvertKeyInput", ParDo.of(convertToKeyedInputs()));
        }


        private static DoFn<KV<JsonObject, KV<Timeseries.DataPoint, UnifiedBinary.UnifiedBinaryEntry>>, KV<KeyPoint, Set<Inputs>>>
        convertToKeyedInputs() {
            return new DoFn<KV<JsonObject, KV<Timeseries.DataPoint, UnifiedBinary.UnifiedBinaryEntry>>, KV<KeyPoint, Set<Inputs>>>() {
                @ProcessElement
                public void convert(ProcessContext context) {
                    var kvPoint = context.element().getValue();
                    var dataPoint = kvPoint.getKey();

                    UnifiedBinary.UnifiedBinaryEntry entry = kvPoint.getValue();
                    Set<Inputs> i = new HashSet<>();
                    switch (entry.getEntryDataCase()) {
                        case TEMPERATURE:
                            entry.getTemperature().getTemperaturesList().forEach(t -> {
                                if (t.getLocation().getNumber() == SKIN_LOCATION)
                                    i.add(Inputs.SKIN_TEMP);
                                if (t.getLocation().getNumber() == AMBIENT_LOCATION)
                                    i.add(Inputs.AMBIENT_TEMP);
                            });
                            break;
                        case ACCEL_MOTION:
                            i.add(Inputs.AC_MOTION);
                            break;
                        case HEART_RATE:
                            i.add(Inputs.HEART_RATE);
                            i.add(Inputs.HR_CONFIDENCE);
                            break;
                        case RESP_RATE:
                            i.add(Inputs.RESP_RATE);
                            i.add(Inputs.RR_CONFIDENCE);
                            break;
                        default:
                            break;
                    }

                    int dutyCycle = dataPoint.getAlgorithmFeatureSet() != null ? dataPoint.getAlgorithmFeatureSet().getAccelDutyCycle() : 0;
                    var keyPoint = KeyPoint.of(Long.parseLong(dataPoint.getUserId()), dataPoint.getDeviceId(),
                            dataPoint.getTimestamp(), dutyCycle);

                    if (!i.isEmpty()) {
                        context.output(KV.of(keyPoint, i));
                    }
                }
            };
        }
    }

    private static class InputCheckerFn extends DoFn<KV<KeyPoint, Set<Inputs>>, KeyPoint> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            var element = context.element();
            KeyPoint key = element.getKey();
            Set<Inputs> inputs = element.getValue();

            if (inputComplete(inputs)) {
                context.output(key);
            }
        }

        private boolean inputComplete(Set<Inputs> inputs) {
            return inputs.containsAll(EnumSet.allOf(Inputs.class));
        }
    }

    private static class InputMerger extends Combine.CombineFn<Set<Inputs>, Set<Inputs>, Set<Inputs>> {
        @Override
        public Set<Inputs> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<Inputs> addInput(Set<Inputs> seenInputs, Set<Inputs> incoming) {
            if (seenInputs != null)
                seenInputs.addAll(incoming);
            return seenInputs;
        }

        @Override
        public Set<Inputs> mergeAccumulators(Iterable<Set<Inputs>> groupedInputs) {
            Set<Inputs> mergedInputs = new HashSet<>();
            groupedInputs.forEach(mergedInputs::addAll);
            return mergedInputs;
        }

        @Override
        public Set<Inputs> extractOutput(Set<Inputs> seenInputs) {
            return seenInputs;
        }
    }

    private static class PubsubNotificator extends DoFn<KeyPoint, PubsubMessage> {
        @ProcessElement
        public void convert(ProcessContext ctx) {
            KeyPoint key = ctx.element();

            Map<String, String> attributes = new HashMap<>();
            attributes.put("user_id", String.valueOf(key.getUserId()));
            attributes.put("device_id", key.getDeviceId());
            attributes.put("timestamp", String.valueOf(key.getTimestamp()));
            attributes.put("accel_duty_cycle", String.valueOf(key.getAccelDutyCycle()));

            var empty = new byte[]{};
            ctx.output(new PubsubMessage(empty, attributes));
        }
    }
}
