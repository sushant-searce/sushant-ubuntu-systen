package com.striiv.dataflow;

import com.google.gson.JsonObject;
import com.striiv.dataflow.adapters.algoparams.ConfigurableParamsServiceWebAdapter;
import com.striiv.dataflow.coders.JsonObjectCoder;
import com.striiv.dataflow.constants.BigtableConstants;
import com.striiv.dataflow.transforms.AccelBasedValueTransforms;
import com.striiv.dataflow.transforms.AccelBasedValueTransforms.MakeAccelHeartRateMutations;
import com.striiv.dataflow.transforms.BrokenTempSensorTransforms;
import com.striiv.dataflow.transforms.GenericFileTransforms;
import com.striiv.dataflow.transforms.TemperatureTransforms;
import com.striiv.dataflow.transforms.extended_sensor_data_transforms.AlertsTransforms;
import com.striiv.proto.binary.AlgorithmVersionOuterClass;
import com.striiv.proto.binary.HeartRate;
import com.striiv.proto.binary.UnifiedBinary;
import com.striiv.proto.core.Timeseries;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import static com.striiv.dataflow.transforms.AggregateDataTransforms.publishDataMessages;

public class SensorDataPipeline extends BaseDataflowPipeline
{
    private final SensorDataPipelineOptions options;
    private static final String STRIP_OFF_UNIFIED_BINARY_ENTRY = "StripOffUnifiedBinaryEntry";

    private final BigtableIO.Write bigtableWriter;
    public static final String SENSOR_DATA_TABLE_NAME = "SensorData";

    public static void main(String[] args)
    {
        SensorDataPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(SensorDataPipelineOptions.class);

        new SensorDataPipeline(options).run();
    }

    SensorDataPipeline(SensorDataPipeline.SensorDataPipelineOptions options)
    {
        this.options = options;

        this.bigtableWriter = BigtableIO.write()
                .withProjectId(options.getProject())
                .withInstanceId("timeseries")
                .withTableId(SENSOR_DATA_TABLE_NAME);
    }

    public interface SensorDataPipelineOptions extends BaseDataflowPipelineOptions
    {

        @Description("Pub/sub topic for extended sensor data processing in streaming mode. " +
                "This is the fully-qualified path.")
        ValueProvider<String> getExtendedSensorDataInputTopic();
        void setExtendedSensorDataInputTopic(ValueProvider<String> extendedSensorDataInputTopic);

        @Description("Pub/sub topic that this pipeline will publish alert messages to. " +
                "This is the fully-qualified path.")
        ValueProvider<String> getAlertDataTaskTopicName();
        void setAlertDataTaskTopicName(ValueProvider<String> alertDataTaskTopicName);

        @Description("Fully qualified pub/sub topic that this pipeline will publish messages to in order to trigger the " +
                "broken temperature sensor detection cloud function")
        @Validation.Required
        ValueProvider<String> getBrokenTempSensorTopic();
        void setBrokenTempSensorTopic(ValueProvider<String> brokenTempSensorTopic);

        @Description("Fully qualified pub/sub topic that this pipeline will publish messages to in order to trigger the " +
                "minute level data cloud function")
        ValueProvider<String> getDiscreteDataTaskTopic();
        void setDiscreteDataTaskTopic(ValueProvider<String> discreteDataTaskTopic);
    }

    private void run()
    {
        var pipeline = Pipeline.create(options);

        // configurable params are pass around as JSONObject, hence the custom coder.
        pipeline.getCoderRegistry().registerCoderForClass(JsonObject.class, JsonObjectCoder.of());

        PCollection<UnifiedBinary.UnifiedBinaryMessage> messages =
                performReadFromPubSubAndApplyFixedWindow(pipeline, options.getInputTopic());

        PCollectionList<UnifiedBinary.UnifiedBinaryMessage> partitionedMessages =
                partitionUnifiedBinaryMessages(messages);

        processAccelRespRateMessages(partitionedMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.RESP_RATE.getNumber()));

        processAccelRespIntervalMessages(partitionedMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.RESP_INTERVALS.getNumber()));

        processAccelHrIntervalMessages(partitionedMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.HR_INTERVALS.getNumber()));

        processAccelHrMessages(partitionedMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.HEART_RATE.getNumber()));

        processTemperatureMessages(partitionedMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.TEMPERATURE.getNumber()));

        pipeline.run();
    }

    private PCollectionList<UnifiedBinary.UnifiedBinaryMessage> partitionUnifiedBinaryMessages(
            PCollection<UnifiedBinary.UnifiedBinaryMessage> unifiedBinaryMessages)
    {
        return unifiedBinaryMessages.apply("PartitionByType",
                Partition.of(UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.values().length + 1,
                        new GenericFileTransforms.PartitionByEntryDataCase()));
    }

    private void processAccelHrIntervalMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> messages)
    {
        var paramsServiceWebAdapter = new ConfigurableParamsServiceWebAdapter(
                options.getConfigurableParamsServiceHost().get(),
                options.getConfigurableParamsServiceApiKey().get()
        );

        PCollection<KV<AlgorithmVersionOuterClass.AlgorithmVersion,
                KV<Timeseries.DataPoint, HeartRate.HeartbeatIntervalEntry>>> accelHrIntervalKvs =
                messages.apply("ConvertToDataPointAccelHrIntervalKV",
                        new AccelBasedValueTransforms.UnifiedBinaryMessageToDataPointAccelHrIntervalKV(
                                paramsServiceWebAdapter));

        accelHrIntervalKvs.apply("MakeAccelHrIntervalMutations", new AccelBasedValueTransforms
                .MakeAccelHeartbeatIntervalMutations(options.isStreaming()))
                .apply("WriteAccelHrIntervalMutations", bigtableWriter.withWriteResults());
    }

    private void processAccelHrMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> messages)
    {
        var paramsServiceWebAdapter = new ConfigurableParamsServiceWebAdapter(
                options.getConfigurableParamsServiceHost().get(),
                options.getConfigurableParamsServiceApiKey().get()
        );

        var accelHeartRateMessagesKvs =
                messages.apply("ConvertToDataPointAccelHeartRateKV",
                   new AccelBasedValueTransforms.UnifiedBinaryMessageToDataPointAccelHeartRateKV(paramsServiceWebAdapter));

        var heartRateSensorDataEnvelopeMessage =
                accelHeartRateMessagesKvs
                        .apply("MakeHeartRateMessage",
                                ParDo.of(new MakeAccelHeartRateMutations.MakeHeartRateMessage()))
                        .apply("MakeSensorDataEnvelope",
                                ParDo.of(new MakeAccelHeartRateMutations.MakeHeartRateSensorDataEnvelope()));

        var heartRateMessages = heartRateSensorDataEnvelopeMessage
                .apply("GetAccelHeartRateMessages", Keys.create())
                .apply("DistinctAccelHeartRateMessages", Distinct.create());

        PCollection<BigtableWriteResult> writeResults = heartRateSensorDataEnvelopeMessage
                .apply("GetAccelHeartRateSensorDataEnvelope", Values.create())
                .apply("MakeAccelHeartRateMutations", new MakeAccelHeartRateMutations(options.isStreaming()))
                .apply("WriteAccelHeartRateMutationsToBigtable", bigtableWriter.withWriteResults());

        // send them to the extended sensor data pipeline for OffBody computation
        if (options.getExtendedSensorDataInputTopic().isAccessible()) {
            heartRateMessages
                    .apply("WaitOnAccelHeartRateWriteResults", Wait.on(writeResults))
                    .apply("SendAccelHrEntriesToPubSubExtendedSensorData",
                            PubsubIO.writeProtos(UnifiedBinary.UnifiedBinaryMessage.class)
                                    .to(options.getExtendedSensorDataInputTopic()));
        }

        if (options.getDiscreteDataTaskTopic().isAccessible() || options.getAggregateDataTaskTopicName().isAccessible()) {
            publishDataMessages(BigtableConstants.HEART_RATE, heartRateMessages, writeResults,
                    options.getDiscreteDataTaskTopic(), options.getAggregateDataTaskTopicName());
        }

        PCollection<Timeseries.DataPoint> accelHeartRateDataPoints = accelHeartRateMessagesKvs
                .apply("GetAccelHeartRateKvs", Values.create())
                .apply("StripOffAlgorithmVersion", Values.create())
                .apply(STRIP_OFF_UNIFIED_BINARY_ENTRY, Keys.create());

        triggerAlerts(BigtableConstants.HEART_RATE, accelHeartRateDataPoints);
    }

    private void processAccelRespIntervalMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> messages)
    {
        var paramsServiceWebAdapter = new ConfigurableParamsServiceWebAdapter(
                options.getConfigurableParamsServiceHost().get(),
                options.getConfigurableParamsServiceApiKey().get()
        );

        var accelRespIntervalKvs =
                messages.apply("ConvertToDataPointAccelRespIntervalKV",
                        new AccelBasedValueTransforms.UnifiedBinaryMessageToDataPointAccelRespIntervalKV(
                                paramsServiceWebAdapter));

        accelRespIntervalKvs.apply("MakeAccelRespIntervalMutations",
                new AccelBasedValueTransforms.MakeAccelRespIntervalMutations(options.isStreaming()))
                .apply("WriteAccelRespIntervalMutationsToBigTable", bigtableWriter.withWriteResults());
    }

    private void processAccelRespRateMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> messages)
    {
        var paramsServiceWebAdapter = new ConfigurableParamsServiceWebAdapter(
                options.getConfigurableParamsServiceHost().get(),
                options.getConfigurableParamsServiceApiKey().get()
        );

        var accelRespRateMessagesAndKvs =
                messages.apply("ConvertToDataPointAccelRespRateKV",
                        new AccelBasedValueTransforms.UnifiedBinaryMessageToDataPointAccelRespRateKV(
                                paramsServiceWebAdapter));

        var respRateSensorDataEnvelopeMessage =
                accelRespRateMessagesAndKvs
                        .apply("MakeRespRateMessage",
                                ParDo.of(new AccelBasedValueTransforms.MakeAccelRespRateMutations.MakeRespRateMessage()))
                        .apply("MakeRespRateSensorDataEnvelope",
                                ParDo.of(new AccelBasedValueTransforms.MakeAccelRespRateMutations.MakeRespRateSensorDataEnvelope()));

        var respMessages = respRateSensorDataEnvelopeMessage
                    .apply("GetRespMessages", Keys.create())
                    .apply("DistinctRespirationMessages", Distinct.create());


        PCollection<BigtableWriteResult> writeResults = respRateSensorDataEnvelopeMessage
            .apply("GetRespRateSensorDataEnvelope", Values.create())
            .apply("MakeAccelRespRateMutations",
                    new AccelBasedValueTransforms.MakeAccelRespRateMutations(options.isStreaming()))
            .apply("WriteAccelRespRateMutationsToBigTable", bigtableWriter.withWriteResults());

        // send them to extended sensor data pipeline for OffBody computation
        if (options.getExtendedSensorDataInputTopic().isAccessible()) {
            respMessages
                    .apply("WaitOnAccelRespRateWriteResults", Wait.on(writeResults))
                    .apply("SendAccelRespRateEntriesToPubSubExtendedSensorData",
                            PubsubIO.writeProtos(UnifiedBinary.UnifiedBinaryMessage.class)
                                    .to(options.getExtendedSensorDataInputTopic()));
        }

        if (options.getDiscreteDataTaskTopic().isAccessible() || options.getAggregateDataTaskTopicName().isAccessible()) {
            publishDataMessages(BigtableConstants.RESP_RATE, respMessages, writeResults, options.getDiscreteDataTaskTopic(),
                    options.getAggregateDataTaskTopicName());
        }

        PCollection<Timeseries.DataPoint> accelRespRateDataPoints = accelRespRateMessagesAndKvs
                .apply("GetAccelRespRateKvs", Values.create())
                .apply("StripOffAlgorithmVersion", Values.create())
                .apply(STRIP_OFF_UNIFIED_BINARY_ENTRY, Keys.create());

        triggerAlerts(BigtableConstants.RESP_RATE, accelRespRateDataPoints);
    }

    private void processTemperatureMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> messages)
    {
        var paramsServiceWebAdapter = new ConfigurableParamsServiceWebAdapter(
                options.getConfigurableParamsServiceHost().get(),
                options.getConfigurableParamsServiceApiKey().get()
        );

        var temperatureKvs =
                messages.apply("ConvertToDataPointTemperatureKV",
                new TemperatureTransforms.UnifiedBinaryMessageAndDataPointTemperatureKV(paramsServiceWebAdapter));

        var temperatureMessageEnvelope =
                temperatureKvs.apply("MakeTemperatureMessage",
                                ParDo.of(new TemperatureTransforms.MakeTemperatureMutations.MakeTemperatureMessage()))
                        .apply("MakeTemperatureEnvelope", ParDo.of(
                                new TemperatureTransforms.MakeTemperatureMutations.MakeTemperatureSensorDataEnvelope()));

        var messagesProcessed = temperatureMessageEnvelope
                .apply("GetTemperatureMessage", Keys.create())
                .apply("Distinct Messages", Distinct.create());

        PCollection<BigtableWriteResult> writeResults = temperatureMessageEnvelope
                .apply("GetKVSensorDataEnvelope", Values.create())
                .apply("MakeTemperatureMutations",
                new TemperatureTransforms.MakeTemperatureMutations(options.isStreaming()))
                .apply("WriteTemperatureMutationsToBigTable", bigtableWriter.withWriteResults());

        // send them to extended sensor data pipeline for OffBody computation
        if (options.getExtendedSensorDataInputTopic().isAccessible()) {
            messagesProcessed
                    .apply("WaitOnTemperatureWriteResults", Wait.on(writeResults))
                    .apply("SendTemperatureEntriesToPubSubExtendedSensorData",
                            PubsubIO.writeProtos(UnifiedBinary.UnifiedBinaryMessage.class)
                                    .to(options.getExtendedSensorDataInputTopic()));

        }

        if (options.getDiscreteDataTaskTopic().isAccessible() || options.getAggregateDataTaskTopicName().isAccessible()) {
            publishDataMessages(BigtableConstants.TMP_SKIN, messagesProcessed, writeResults,
                    options.getDiscreteDataTaskTopic(), options.getAggregateDataTaskTopicName());
        }

        if (options.getAggregateDataTaskTopicName().isAccessible()) {
            // Use temperature as trigger for cough data
            publishDataMessages(BigtableConstants.COUGH, messagesProcessed, writeResults, null,
                    options.getAggregateDataTaskTopicName());
        }

        PCollection<Timeseries.DataPoint> temperatureDataPoints = temperatureKvs
                .apply("GetKVTemperatureDataPoints", Values.create())
                .apply(STRIP_OFF_UNIFIED_BINARY_ENTRY, Keys.create());

        if (options.getBrokenTempSensorTopic().isAccessible())
        {
            temperatureDataPoints
                    .apply("TriggerBrokenTemperatureDetection", new BrokenTempSensorTransforms.TriggerBrokenTempSensor())
                    .apply("PublishBrokenTempSensorMessages", PubsubIO.writeMessages().to(options.getBrokenTempSensorTopic().get()));
        }

        triggerAlerts(BigtableConstants.TMP_SKIN, temperatureDataPoints);
    }

    private void triggerAlerts(String dataType, PCollection<Timeseries.DataPoint> dataPoints)
    {
        if (!options.getAlertDataTaskTopicName().isAccessible())
            return;

        dataPoints
                .apply(String.format("Trigger%sAlerts", dataType), new AlertsTransforms.TriggerPeriodicAlert(dataType))
                .apply(String.format("Write%sAlertMessages", dataType),
                        PubsubIO.writeMessages().to(options.getAlertDataTaskTopicName()));
    }
}