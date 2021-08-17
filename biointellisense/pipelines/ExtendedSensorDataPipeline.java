package com.striiv.dataflow;

import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import com.google.gson.JsonObject;
import com.striiv.dataflow.adapters.algoparams.ConfigurableParamsServiceWebAdapter;
import com.striiv.dataflow.coders.JsonObjectCoder;
import com.striiv.dataflow.config.BigtableInstanceConfig;
import com.striiv.dataflow.constants.BigtableConstants;
import com.striiv.dataflow.transforms.DataPointTransforms;
import com.striiv.dataflow.transforms.GenericFileTransforms;
import com.striiv.dataflow.transforms.extended_sensor_data_transforms.*;
import com.striiv.proto.binary.BodyPosition;
import com.striiv.proto.binary.UnifiedBinary;
import com.striiv.proto.core.Timeseries;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import static com.striiv.dataflow.transforms.AggregateDataTransforms.publishDataMessages;

public class ExtendedSensorDataPipeline extends BaseDataflowPipeline
{
    private final ExtendedSensorDataPipeline.ExtendedSensorDataPipelineOptions options;
    private final CloudBigtableScanConfiguration extendedSensorDataBigtableConfig;
    public static final String EXTENDED_SENSOR_DATA_TABLE_NAME = "ExtendedSensorData";

    private final BigtableIO.Write bigtableWriter;

    public static void main(String[] args)
    {
        ExtendedSensorDataPipeline.ExtendedSensorDataPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(ExtendedSensorDataPipeline.ExtendedSensorDataPipelineOptions.class);

        new ExtendedSensorDataPipeline(options).run();
    }

    private ExtendedSensorDataPipeline(ExtendedSensorDataPipeline.ExtendedSensorDataPipelineOptions options)
    {
        this.options = options;

        this.bigtableWriter = BigtableIO.write()
                .withProjectId(options.getProject())
                .withInstanceId("timeseries")
                .withTableId(EXTENDED_SENSOR_DATA_TABLE_NAME);

        var extendedSensorDataConfig = new BigtableInstanceConfig(options.getProject(),
                "timeseries", EXTENDED_SENSOR_DATA_TABLE_NAME);
        this.extendedSensorDataBigtableConfig = extendedSensorDataConfig.getCloudBigtableScanConfiguration();
    }

    public interface ExtendedSensorDataPipelineOptions extends BaseDataflowPipelineOptions {}

    private void run()
    {
        var pipeline = Pipeline.create(options);

        // configurable params are pass around as JSONObject, hence the custom coder.
        pipeline.getCoderRegistry().registerCoderForClass(JsonObject.class, JsonObjectCoder.of());

        PCollection<UnifiedBinary.UnifiedBinaryMessage> messages =
                performReadFromPubSubAndApplyFixedWindow(pipeline, options.getInputTopic());

        PCollectionList<UnifiedBinary.UnifiedBinaryMessage> partitionedMessages =
                partitionUnifiedBinaryMessages(messages);

        processBodyPositionCalibrationMessages(partitionedMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.BODY_POSITION_CALIBRATION.getNumber()));

        processAccelMotionMessages(partitionedMessages
                .get(UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.ACCEL_MOTION.getNumber()));

        processContactTraceMessages(partitionedMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.CONTACT_ENTRY.getNumber()));

        processRespBandEnergyMessages(partitionedMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.RESP_BAND_ENERGY.getNumber()));
        
        processStepIntervalMessages(partitionedMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.STEP_INTERVALS.getNumber()));

        processStepCountMessages(partitionedMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.STEP_COUNT.getNumber()));
        
        pipeline.run();
    }

    private PCollectionList<UnifiedBinary.UnifiedBinaryMessage> partitionUnifiedBinaryMessages(
            PCollection<UnifiedBinary.UnifiedBinaryMessage> messages)
    {
        return messages.apply("PartitionByType",
                Partition.of(UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.values().length + 2,
                        new GenericFileTransforms.PartitionByEntryDataCase()));
    }

    private void processBodyPositionCalibrationMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> messages)
    {
        var paramsServiceWebAdapter = new ConfigurableParamsServiceWebAdapter(
                options.getConfigurableParamsServiceHost().get(),
                options.getConfigurableParamsServiceApiKey().get()
        );

        PCollection<KV<Timeseries.DataPoint, BodyPosition.BodyPositionCalibrationEntry>> bodyPositionCalibrationKvs =
                messages.apply("ConvertToDataPointBodyPositionCalibrationKV",
                        new BodyPositionCalibrationTransforms.UnifiedBinaryMessageToDataPointBodyPositionCalibrationKV(
                                paramsServiceWebAdapter));

        bodyPositionCalibrationKvs.apply("MakeBodyPositionCalibrationMutations",
                new BodyPositionCalibrationTransforms.MakeBodyPositionCalibrationMutations(options.isStreaming()))
                .apply("WriteBodyPositionCalibrationMutations", bigtableWriter.withWriteResults());
    }
    
    private void processAccelMotionMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> messages)
    {
        PCollection<Timeseries.DataPoint> accelDataPoints = messages
                .apply("CreateAccelDataPoints",
                        ParDo.of(new DataPointTransforms.CreateAcAccelDataPoints()));

        processAccelMotionMessagesIntoActivityLevel(accelDataPoints, messages);
        processAccelMotionMessagesIntoSleep(accelDataPoints);
    }

    private void processAccelMotionMessagesIntoActivityLevel(PCollection<Timeseries.DataPoint> accelDataPoints,
                                                             PCollection<UnifiedBinary.UnifiedBinaryMessage> messages)
    {
        PCollection<BigtableWriteResult> writeResults = accelDataPoints
                .apply("RunActivityLevelClassificationAlgorithm",
                        new ActivityLevelTransforms.RunActivityLevelClassification(options))
                .apply("WriteActivityLevelMutations", bigtableWriter.withWriteResults());

        publishDataMessages(BigtableConstants.ACTIVITY_LEVEL, messages, writeResults, null,
                options.getAggregateDataTaskTopicName());
    }

    private void processAccelMotionMessagesIntoSleep(PCollection<Timeseries.DataPoint> accelDataPoints)
    {
        accelDataPoints
                .apply("RunSleepDetectionAlgorithm",
                        new SleepTransforms.RunSleepDetection(options, extendedSensorDataBigtableConfig))
                .apply("WriteSleepStateMutations", bigtableWriter.withWriteResults());
    }

    private void processContactTraceMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> contactTraceMessages)
    {
        contactTraceMessages
                .apply("MakeContactTraceMutations", new ContactTraceTransforms.MakeContactTraceMutations())
                .apply("WriteContactTraceMutations", bigtableWriter.withWriteResults());
    }

    private void processRespBandEnergyMessages (PCollection<UnifiedBinary.UnifiedBinaryMessage> respBandEnergyMessages)
    {
        respBandEnergyMessages
                .apply("MakeRespBandEnergyMutations",
                        new RespiratoryBandEnergyTransforms.MakeRespBandEnergyMutations())
                .apply("WriteRespBandEnergyMutations", bigtableWriter.withWriteResults());
    }
    
    private void processStepIntervalMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> stepIntervalMessages) {
        stepIntervalMessages
                .apply("MakeStepIntervalMutations", new StepIntervalTransforms.MakeStepIntervalMutations())
                .apply("WriteStepIntervalMutations", bigtableWriter.withWriteResults());

    }

    private void processStepCountMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> stepCountMessages) {
        stepCountMessages
                .apply("MakeStepCountMutations", new StepCountTransforms.MakeStepCountMutations())
                .apply("WriteStepCountMutations", bigtableWriter.withWriteResults());

    }
}