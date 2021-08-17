package com.striiv.dataflow;

import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.striiv.dataflow.BaseDataflowPipeline.BaseDataflowPipelineOptions;
import com.striiv.dataflow.adapters.algoparams.ConfigurableParamsServiceWebAdapter;
import com.striiv.dataflow.coders.JsonObjectCoder;
import com.striiv.dataflow.config.BigQueryTableConfig;
import com.striiv.dataflow.config.BigtableInstanceConfig;
import com.striiv.dataflow.transforms.DatastreamTransforms;
import com.striiv.dataflow.utils.AudioUtils;
import com.striiv.dataflow.utils.TimeDriftOffsetUtils;
import com.striiv.proto.binary.DataStream;
import com.striiv.proto.binary.UnifiedBinary;
import com.striiv.proto.core.Timeseries;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static com.striiv.dataflow.constants.BigQueryConstants.INITIAL_TIMESTAMP;
import static com.striiv.dataflow.utils.AudioUtils.writeWAV;

/**
 * This pipeline takes UnifiedBinaryEntry files containing DataStream objects, parses the DataStream objects, and writes
 * them to BigQuery.
 */
public class DatastreamPipeline {

    public static void main(String[] args) {
        DatastreamPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(DatastreamPipelineOptions.class);

        new DatastreamPipeline(options).run();
    }

    private static final String BIGQUERY_TABLE_SCHEMA_PATH = "user_data/data_stream.json";
    private final DatastreamPipelineOptions options;

    private DatastreamPipeline(DatastreamPipelineOptions options) {
        this.options = options;
    }

    public interface DatastreamPipelineOptions extends BaseDataflowPipelineOptions {

        @Description("Text file that the pipeline will read from.  Each line of the file should be newline-delimited " +
                "JSON, where each line contains a user ID, device ID, and a base64-encoded UnifiedBinaryEntry file.  " +
                "Required if the pipeline is being run in batch mode.")
        ValueProvider<String> getInputFile();
        void setInputFile(ValueProvider<String> inputFile);

        @Description("BigQuery table that the pipeline will write to.")
        @Default.String("user_data.DataStream")
        ValueProvider<String> getDestination();
        void setDestination(ValueProvider<String> destination);

        @Description("GCS Bucket for audio snippets.")
        @Default.String("gs://striiv-api-staging-audio-files")
        ValueProvider<String> getAudioBucket();
        void setAudioBucket(ValueProvider<String> gcsBucket);

    }

    private void run() {
        Pipeline pipeline = Pipeline.create(options);

        // configurable params are pass around as JSONObject, hence the custom coder.
        pipeline.getCoderRegistry().registerCoderForClass(JsonObject.class, JsonObjectCoder.of());

        BigtableInstanceConfig datastreamConfig = new BigtableInstanceConfig(options.getProject(),"timeseries", "DataStream");
        CloudBigtableScanConfiguration bigtableScanConfiguration = datastreamConfig.getCloudBigtableScanConfiguration();

        PCollection<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>> dataStreamKv = getDataStreamEntries(pipeline);

        dataStreamKv
                .apply("FilterInvalidFirstEntryTimestamps", ParDo.of(new DatastreamTransforms.FilterInvalidFirstEntryTimestamps()))
                .apply("WriteToBigQuery", createBigQueryWrite());

        int dataFieldsCaseSize = DataStream.DataStreamEntry.DataFieldsCase.values().length;

        PCollectionList<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>> partitionedEntries = dataStreamKv
                .apply("PartitionByDataType", Partition.of(dataFieldsCaseSize, new DatastreamTransforms.PartitionByDataFieldsCase()));

        PCollection<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>> acousticDataStreamKv = partitionedEntries
                .get(DatastreamTransforms.PartitionByDataFieldsCase.
                        getPartitionIndex(DataStream.DataStreamEntry.DataFieldsCase.ACOUSTIC_DATA));

        processAcousticData(acousticDataStreamKv);

        partitionedEntries
                .apply("MakeDataStreamMutations", new DatastreamTransforms.MakeDatastreamMutations())
                .apply("WriteDataStreamToBigtable", CloudBigtableIO.writeToTable(bigtableScanConfiguration));

        pipeline.run();
    }

    private PCollection<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>> getDataStreamEntries(Pipeline pipeline) {
        if (options.isStreaming()) {
            return getStreamingDataStreamEntries(pipeline);
        } else {
            return getBatchDataStreamEntries(pipeline);
        }
    }

    private PCollection<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>> getStreamingDataStreamEntries(
            Pipeline pipeline) {
        ConfigurableParamsServiceWebAdapter paramsServiceWebAdapter = new ConfigurableParamsServiceWebAdapter(
                options.getConfigurableParamsServiceHost().get(), options.getConfigurableParamsServiceApiKey().get());

        return pipeline.apply("ReadUnifiedBinaryMessagesFromPubSub",
                        PubsubIO.readProtos(UnifiedBinary.UnifiedBinaryMessage.class)
                                .fromTopic(options.getInputTopic()))
                .apply("ConvertToDataPointDataStreamKV",
                        new DatastreamTransforms.UnifiedBinaryMessageToDataPointDataStreamKV(paramsServiceWebAdapter));
    }

    private PCollection<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>> getBatchDataStreamEntries(
            Pipeline pipeline) {
        return pipeline
                .apply("ReadFromGCS", TextIO.read().from(options.getInputFile()))
                .apply("ConvertToDataPointDataStreamKV", new DatastreamTransforms.JsonToDataPointDataStreamKV());
    }

    private BigQueryIO.Write<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>> createBigQueryWrite() {
        BigQueryTableConfig config = getBigQueryTableConfig();
        BigQueryIO.Write<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>> write =
                makeBigQueryWriteFromConfig(config);
        return write.withTimePartitioning(getTimePartitioning());
    }

    private BigQueryTableConfig getBigQueryTableConfig() {
        return BigQueryTableConfig.createBigQueryTableConfig(options.getProject(), BIGQUERY_TABLE_SCHEMA_PATH);
    }

    private BigQueryIO.Write<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>> makeBigQueryWriteFromConfig(
            BigQueryTableConfig config) {
        if (options.isStreaming()) {
            return makeStreamingBigQueryWrite(config);
        } else {
            return makeBatchBigQueryWrite(config);
        }
    }

    private BigQueryIO.Write<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>> makeStreamingBigQueryWrite(
            BigQueryTableConfig config) {
        return BigQueryIO.<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>>write()
                .to(options.getDestination())
                .withSchema(config.getTableSchema())
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withFormatFunction(new DatastreamTransforms.FormatDataStreamAsTableRow());
    }

    private BigQueryIO.Write<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>> makeBatchBigQueryWrite(
            BigQueryTableConfig config) {
        return BigQueryIO.<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>>write()
                .to(options.getDestination())
                .withSchema(config.getTableSchema())
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withFormatFunction(new DatastreamTransforms.FormatDataStreamAsTableRow());
    }

    private static TimePartitioning getTimePartitioning() {
        TimePartitioning timePartitioning = new TimePartitioning();
        timePartitioning.setField(INITIAL_TIMESTAMP);
        return timePartitioning;
    }

    private void processAcousticData(PCollection<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>> dataStreamKv) {
        dataStreamKv
                .apply("TagDataStreamEntryKVWithSnippetID", ParDo.of(new TagDataStreamEntryWithSnippetId()))
                .apply(Window.into(Sessions.withGapDuration(Duration.standardSeconds(5))))
                .apply("GroupBySnippetId", GroupByKey.<ByteString, KV<Timeseries.DataPoint, DataStream.DataStreamEntry>>create())
                .apply(ParDo.of(new WriteWaveFn()));
    }

    static class TagDataStreamEntryWithSnippetId extends DoFn<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>, KV<ByteString, KV<Timeseries.DataPoint, DataStream.DataStreamEntry>>> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            DataStream.DataStreamEntry dataStreamEntry = context.element().getValue();
            ByteString snippetId = dataStreamEntry.getSnippetId();

            context.output(KV.of(snippetId, context.element()));
        }
    }

    static class WriteWaveFn extends DoFn<KV<ByteString, Iterable<KV<Timeseries.DataPoint,
                DataStream.DataStreamEntry>>>, Void> {

        @ProcessElement
        public void processElement(ProcessContext context) {
            Comparator<KV<Long, DataStream.DataStreamEntry>> orderByKey = new KV.OrderByKey<>();

            Iterable<KV<Timeseries.DataPoint, DataStream.DataStreamEntry>> kvIterable = context.element().getValue();
            Timeseries.DataPoint dataPoint = kvIterable.iterator().next().getKey();
            String userId = dataPoint.getUserId();
            String deviceId = dataPoint.getDeviceId();

            int timeDrift;
            Long timestampBase;

            ArrayList<KV<Long, DataStream.DataStreamEntry>> dataStreamEntryKvList = new ArrayList<>();

            for (KV<Timeseries.DataPoint, DataStream.DataStreamEntry> dataPointDataStreamEntryKV : kvIterable) {
                Timeseries.DataPoint snippetDataPoint = dataPointDataStreamEntryKV.getKey();

                timestampBase = dataPointDataStreamEntryKV.getValue().getUnixUsTimestampFirstEntry();
                timeDrift = TimeDriftOffsetUtils.getUnitAdjustedTimeDrift(
                        TimeUnit.MILLISECONDS, snippetDataPoint.getUnifiedBinaryMeta().getTimeDriftToApply());

                DataStream.DataStreamEntry updatedTsEntry = dataPointDataStreamEntryKV.getValue().toBuilder()
                        .setUnixUsTimestampFirstEntry(timestampBase + timeDrift )
                        .build();
                dataStreamEntryKvList.add(KV.of((timestampBase + timeDrift), updatedTsEntry));
            }

            dataStreamEntryKvList.sort(orderByKey);
            String audioBucket = context.getPipelineOptions().as(DatastreamPipelineOptions.class).getAudioBucket().get();

            long timestampFirstEntry = dataStreamEntryKvList.get(0).getKey() / 1000;
            String filePath = String.format("%s/%s-%s/snd_%d.wav", audioBucket, userId, deviceId, timestampFirstEntry);
            try {
                OutputStream output = AudioUtils.createWAVOutputStream(filePath);
                writeWAV(dataStreamEntryKvList, output);
            }
            catch (IOException e) {
                throw new AudioUtils.WAVException("Failed to create WAV file.", e);
            }
        }
    }

}
