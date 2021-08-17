package com.striiv.dataflow.filedigest;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.striiv.dataflow.adapters.encryption.EncryptionServiceWebAdapter;
import com.striiv.dataflow.coders.JsonObjectCoder;
import com.striiv.dataflow.config.BigQueryTableConfig;
import com.striiv.dataflow.config.BigtableInstanceConfig;
import com.striiv.dataflow.transforms.EncryptionTransforms;
import com.striiv.dataflow.transforms.GenericFileTransforms;
import com.striiv.proto.binary.UnifiedBinary;
import com.striiv.proto.binary.UnifiedBinary.UnifiedBinaryMessage;
import com.striiv.proto.core.ParsedGenericFileMessage;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.*;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.striiv.dataflow.transforms.EncryptionTransforms.DECRYPTED_FILES;
import static com.striiv.dataflow.transforms.GenericFileTransforms.UNIFIED_BINARY_MESSAGES;
import static com.striiv.dataflow.transforms.GenericFileTransforms.UNPARSEABLE_UNIFIED_BINARY_MESSAGES;
import static com.striiv.proto.core.ParsedGenericFileMessage.FileType.ENCRYPTED_FILE_VALUE;
import static com.striiv.proto.core.ParsedGenericFileMessage.FileType.PROTOCOL_BUFFER_VALUE;

public abstract class FileDigestPipeline {
    protected final FileDigestPipelineOptions options;
    protected final BigtableInstanceConfig timeseriesInstanceConfig;

    protected final CloudBigtableScanConfiguration bigtableScanConfiguration;
    protected final CloudBigtableScanConfiguration intradayBigtableScanConfiguration;
    protected final String pipelineRunner;

    private final EncryptionTransforms.DecryptFile.Decryptor decryptor;

    protected FileDigestPipeline(FileDigestPipelineOptions options) {
        this.options = options;

        this.timeseriesInstanceConfig = makeBigtableInstanceConfig("TimeSeriesData");
        var intradayConfig = makeBigtableInstanceConfig("Intraday");

        this.bigtableScanConfiguration = timeseriesInstanceConfig.getCloudBigtableScanConfiguration();
        this.intradayBigtableScanConfiguration = intradayConfig.getCloudBigtableScanConfiguration();

        this.decryptor = new EncryptionServiceWebAdapter(options.getEncryptionServiceHost(),
                options.getEncryptionServiceApiKey());

        this.pipelineRunner = options.getRunner().getSimpleName();
    }

    protected BigtableInstanceConfig makeBigtableInstanceConfig(String tableId) {
        return new BigtableInstanceConfig(getProject(), getBigtableInstanceId(), tableId);
    }

    protected String getProject() {
        return options.getProject();
    }

    private String getBigtableInstanceId() {
        return options.getBigtableInstanceId();
    }

    public static void main(String[] args) {
        FileDigestPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(FileDigestPipelineOptions.class);

        var factory = new FileDigestPipelineFactory();
        var fileDigestPipeline = factory.create(options);

        fileDigestPipeline.run();
    }

    private PipelineResult run() {
        var pipeline = Pipeline.create(options);
        // configurable params are pass around as JSONObject, hence the custom coder.
        pipeline.getCoderRegistry().registerCoderForClass(JsonObject.class, JsonObjectCoder.of());

        applyTransforms(pipeline);
        return pipeline.run();
    }

    private void applyTransforms(Pipeline pipeline) {
        PCollectionTuple parseResults = readAndParseInputs(pipeline);
        logUnparseableFiles(parseResults);

        PCollection<ParsedGenericFileMessage.ParsedGenericFile> parsedGenericFiles = getParsedGenericFiles(
                parseResults);
        parsedGenericFiles = filterParsedGenericFiles(parsedGenericFiles);

        syncUserIds(parsedGenericFiles);
        logParsedGenericFiles(parsedGenericFiles);

        PCollection<ParsedGenericFileMessage.ParsedGenericFile> timeDriftExtractedFiles =
                detectTimeDriftOffset(parsedGenericFiles, options);

        PCollectionList<ParsedGenericFileMessage.ParsedGenericFile> partitionedParsedGenericFiles =
                partitionParsedGenericFiles(timeDriftExtractedFiles);

        PCollectionTuple parseEncryptedFileResults = parseEncryptedFiles(
                partitionedParsedGenericFiles.get(ENCRYPTED_FILE_VALUE));
        logUnparseableEncryptedFiles(parseEncryptedFileResults);

        PCollectionTuple decryptFileResults = decryptFiles(parseEncryptedFileResults);
        logUndecryptableFiles(decryptFileResults);

        PCollectionTuple parseDecryptedUnifiedBinaryResults = parseDecryptedFiles(
                decryptFileResults.get(DECRYPTED_FILES));
        PCollectionTuple parseUnifiedBinaryResults = parseUnifiedBinaries(
                partitionedParsedGenericFiles.get(PROTOCOL_BUFFER_VALUE));
        logUnparseableUnifiedBinaryMessages(parseDecryptedUnifiedBinaryResults, parseUnifiedBinaryResults);

        PCollection<UnifiedBinary.UnifiedBinaryMessage> unifiedBinaryMessages = mergeParseResults(
                parseDecryptedUnifiedBinaryResults, parseUnifiedBinaryResults);

        processDeviceActivationMessages(unifiedBinaryMessages);

        unifiedBinaryMessages = preprocessUnifiedBinaryMessages(unifiedBinaryMessages);
        logUnifiedBinaryEntries(unifiedBinaryMessages);

        PCollectionList<UnifiedBinary.UnifiedBinaryMessage> partitionedUnifiedBinaryMessages =
                partitionUnifiedBinaryMessages(unifiedBinaryMessages);

        processAcAccelMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.ACCEL_MOTION.getNumber()));

        processMessageLogMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.LOG.getNumber()));

        processEventLogMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.EVENT_LOG.getNumber()));

        processBbiEntryMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.BBI_ENTRY.getNumber()));

        processDatastreamMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.DATA_STREAM.getNumber()));

        processAccelRespRateMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.RESP_RATE.getNumber()));

        processAccelRespIntervalMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.RESP_INTERVALS.getNumber()));

        processAccelHrIntervalMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.HR_INTERVALS.getNumber()));

        processAccelHrMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.HEART_RATE.getNumber()));

        processTemperatureMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.TEMPERATURE.getNumber()));

        processBodyPositionCalibrationMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.BODY_POSITION_CALIBRATION.getNumber()));

        processBodyPositionMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.BODY_POSITION.getNumber()));

        processLowMotionMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.LOW_MOTION.getNumber()));
        
        processContactTraceMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.CONTACT_ENTRY.getNumber()));

        processRespBandEnergyMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.RESP_BAND_ENERGY.getNumber()));
        
        processStepIntervalMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.STEP_INTERVALS.getNumber()));

        processStepCountMessages(partitionedUnifiedBinaryMessages.get(
                UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.STEP_COUNT.getNumber()));
    }

    protected abstract PCollectionTuple readAndParseInputs(Pipeline pipeline);

    protected abstract void logUnparseableFiles(PCollectionTuple parseResults);

    protected abstract PCollection<ParsedGenericFileMessage.ParsedGenericFile> getParsedGenericFiles(
            PCollectionTuple parseResults);

    private PCollection<ParsedGenericFileMessage.ParsedGenericFile> filterParsedGenericFiles(
            PCollection<ParsedGenericFileMessage.ParsedGenericFile> parsedGenericFiles) {
        return parsedGenericFiles.apply("PrePartitionFilters", new GenericFileTransforms.PrePartitionFilters());
    }

    private BigQueryTableConfig makePartialBigqueryConfig(String jsonFilename) {
        return BigQueryTableConfig.createBigQueryTableConfig(getProject(), jsonFilename)
                .withWriteMethod(BigQueryIO.Write.Method.DEFAULT);
    }

    protected abstract void syncUserIds(PCollection<ParsedGenericFileMessage.ParsedGenericFile> parsedGenericFiles);

    protected abstract void logParsedGenericFiles(
            PCollection<ParsedGenericFileMessage.ParsedGenericFile> parsedGenericFiles);
    
    protected abstract void processDeviceActivityFromUnifiedBinaryMessage(
    		PCollection<KV<String, UnifiedBinaryMessage>> combinedUnifiedBinaryMessages);

    private PCollectionList<ParsedGenericFileMessage.ParsedGenericFile> partitionParsedGenericFiles(
            PCollection<ParsedGenericFileMessage.ParsedGenericFile> parsedGenericFiles) {
        return parsedGenericFiles.apply(
                "PartitionParsedGenericFilesByFileType", Partition.of(
                        ParsedGenericFileMessage.FileType.values().length,
                        new GenericFileTransforms.PartitionParsedGenericFilesByFileType()));
    }

    protected BigQueryTableConfig makeUserDataBigqueryConfig(String jsonFilename) {
        return makePartialBigqueryConfig(jsonFilename).withDatasetId(options.getBigqueryUserDataDataset());
    }

    protected BigQueryTableConfig makeErrorLogBigqueryConfig(String jsonFilename) {
        return makePartialBigqueryConfig(jsonFilename).withDatasetId(options.getBigqueryErrorLogDataset());
    }

    private PCollectionTuple parseEncryptedFiles(
            PCollection<ParsedGenericFileMessage.ParsedGenericFile> encryptedFiles) {
        return encryptedFiles.apply("ParseEncryptedFile", ParDo.of(new EncryptionTransforms.ParseEncryptedFile())
                .withOutputTags(EncryptionTransforms.ENCRYPTED_FILES,
                        TupleTagList.of(EncryptionTransforms.UNPARSEABLE_ENCRYPTED_FILES)));
    }

    private void logUnparseableEncryptedFiles(PCollectionTuple parseEncryptedFileResults) {
        BigQueryTableConfig config = makeErrorLogBigqueryConfig("error_logs/unparseable_encrypted_files.json");

        parseEncryptedFileResults.get(EncryptionTransforms.UNPARSEABLE_ENCRYPTED_FILES)
                .apply("CreateUnparseableEncryptedFileTablerow", ParDo.of(
                        new EncryptionTransforms.CreateUnparseableEncryptedFileTableRow()))
                .apply("WriteUnparseableEncryptedFileToBigquery", config.getWrite());
    }

    private PCollectionTuple decryptFiles(PCollectionTuple parseEncryptedFileResults) {
        return parseEncryptedFileResults.get(EncryptionTransforms.ENCRYPTED_FILES)
                .apply("DecryptFile", ParDo.of(new EncryptionTransforms.DecryptFile(decryptor))
                        .withOutputTags(DECRYPTED_FILES,
                                TupleTagList.of(EncryptionTransforms.UNDECRYPTABLE_FILES)));
    }

    private void logUndecryptableFiles(PCollectionTuple decryptFileResults) {
        BigQueryTableConfig config = makeErrorLogBigqueryConfig("error_logs/undecryptable_files.json");

        decryptFileResults.get(EncryptionTransforms.UNDECRYPTABLE_FILES)
                .apply("CreateUndecryptableFileTableRow", ParDo.of(
                        new EncryptionTransforms.CreateUndecryptableFileTableRow()))
                .apply("WriteUndecryptableFileToBigquery", config.getWrite());
    }

    private PCollectionTuple parseDecryptedFiles(PCollection<KV<ParsedGenericFileMessage.ParsedGenericFile,
                ByteString>> decryptedFiles) {
        return decryptedFiles.apply("ParseUnifiedBinaryFromDecryptedFiles", ParDo.of(
                new GenericFileTransforms.ParseUnifiedBinaryMessage())
                .withOutputTags(UNIFIED_BINARY_MESSAGES, TupleTagList.of(UNPARSEABLE_UNIFIED_BINARY_MESSAGES)));
    }

    private PCollectionTuple parseUnifiedBinaries(
            PCollection<ParsedGenericFileMessage.ParsedGenericFile> unifiedBinaryMessages) {
        return unifiedBinaryMessages.apply("ParseUnifiedBinaryFromPbFiles", ParDo.of(
                new GenericFileTransforms.ExtractAndParseUnifiedBinaryMessage())
                .withOutputTags(UNIFIED_BINARY_MESSAGES, TupleTagList.of(UNPARSEABLE_UNIFIED_BINARY_MESSAGES)));
    }

    private void logUnparseableUnifiedBinaryMessages(PCollectionTuple ... parseResultTuples) {
        Iterable<PCollection<TableRow>> pCollectionTuples = Arrays.stream(parseResultTuples)
                .map(pCollectionTuple -> pCollectionTuple.get(UNPARSEABLE_UNIFIED_BINARY_MESSAGES))
                .collect(Collectors.toList());

        BigQueryTableConfig config = makeErrorLogBigqueryConfig("unparseable_unified_binary_message_table_schema.json");
        PCollectionList.of(pCollectionTuples)
                .apply("MergeUnparseableUnifiedBinaryMessages", Flatten.pCollections())
                .apply("WriteUnparseableUnifiedBinaryMessageToBigQuery", config.getWrite());
    }

    private PCollection<UnifiedBinary.UnifiedBinaryMessage> mergeParseResults(PCollectionTuple ... parseResultTuples) {
        Iterable<PCollection<UnifiedBinary.UnifiedBinaryMessage>> unifiedBinaryMessageIterable = Arrays
                .stream(parseResultTuples)
                .map(pCollectionTuple -> pCollectionTuple.get(UNIFIED_BINARY_MESSAGES))
                .collect(Collectors.toList());

        return PCollectionList.of(unifiedBinaryMessageIterable)
                .apply("MergeUnifiedBinaryMessages", Flatten.pCollections());
    }

    protected abstract void processDeviceActivationMessages(
            PCollection<UnifiedBinary.UnifiedBinaryMessage> unifiedBinaryMessages);

    protected abstract PCollection<UnifiedBinary.UnifiedBinaryMessage> preprocessUnifiedBinaryMessages(
            PCollection<UnifiedBinary.UnifiedBinaryMessage> unifiedBinaryMessages);

    protected abstract void logUnifiedBinaryEntries(
            PCollection<UnifiedBinary.UnifiedBinaryMessage> unifiedBinaryMessages);

    private PCollectionList<UnifiedBinary.UnifiedBinaryMessage> partitionUnifiedBinaryMessages(
            PCollection<UnifiedBinary.UnifiedBinaryMessage> unifiedBinaryMessages) {
        // length + 2, because the ENTRY_DATA enum skips number 22
        return unifiedBinaryMessages.apply("PartitionByType",
                Partition.of(UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.values().length + 2,
                        new GenericFileTransforms.PartitionByEntryDataCase()));
    }

    public static PCollection<ParsedGenericFileMessage.ParsedGenericFile> detectTimeDriftOffset(
            PCollection<ParsedGenericFileMessage.ParsedGenericFile> parsedGenericFiles, FileDigestPipelineOptions options) {

        return parsedGenericFiles
                .apply("TimeDriftDetection",
                        ParDo.of(new GenericFileTransforms.TimeDriftDetection(options.getTimeDriftOffsetNotificationTopic().get())));
    }

    protected abstract void processAcAccelMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> accelMessages);

    protected abstract void processMessageLogMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage>
                                                              messageLogMessages);

    protected abstract void processEventLogMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> eventLogMessages);

    protected abstract void processBbiEntryMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> bbiEntryMessages);

    protected abstract void processDatastreamMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage>
                                                              datastreamMessages);

    protected abstract void processAccelHrIntervalMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage>
                                                                   accelHrIntervalMessages);

    protected abstract void processAccelRespIntervalMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage>
                                                                     accelRespIntervalMessages);

    protected abstract void processAccelHrMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> accelHrMessages);

    protected abstract void processAccelRespRateMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage>
                                                                 accelRespRateMessages);

    protected abstract void processTemperatureMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage>
                                                               temperatureMessages);

    protected abstract void processBodyPositionMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage>
                                                                bodyPositionMessages);

    protected abstract void processBodyPositionCalibrationMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage>
                                                                   bodyPositionCalibrationMessages);

    protected abstract void processLowMotionMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage>
                                                             unifiedBinaryMessagePCollection);
    
    protected abstract void processContactTraceMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage>
    															contactTraceMessagesPCollection);

    protected abstract void processRespBandEnergyMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage>
                                                                  respBandEnergyMessagesPCollection);
    
    protected abstract void processStepIntervalMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage>
                                                                stepIntervalMessagesPCollection);

    protected abstract void processStepCountMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage>
                                                                stepCountMessagesPCollection);
}