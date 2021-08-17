package com.striiv.dataflow;

import com.google.bigtable.v2.Mutation;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.striiv.dataflow.adapters.algoparams.ConfigurableParamsServiceWebAdapter;
import com.striiv.dataflow.coders.JsonObjectCoder;
import com.striiv.dataflow.config.BigtableInstanceConfig;
import com.striiv.dataflow.constants.BigtableConstants;
import com.striiv.dataflow.constants.RedisConstants;
import com.striiv.dataflow.transforms.*;
import com.striiv.dataflow.transforms.extended_sensor_data_transforms.LowMotionTransforms;
import com.striiv.dataflow.transforms.extended_sensor_data_transforms.OnOffBodyTransforms;
import com.striiv.dataflow.utils.TimestampUtils;
import com.striiv.proto.binary.UnifiedBinary;
import com.striiv.proto.binary.UnifiedBinary.UnifiedBinaryMessage;
import com.striiv.proto.core.Timeseries;
import com.striiv.proto.core.Timeseries.DataPoint;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hbase.util.Pair;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.striiv.dataflow.transforms.extended_sensor_data_transforms.OnOffBodyTransforms.RunOffBodyAlgorithm;
import static com.striiv.dataflow.transforms.extended_sensor_data_transforms.OnOffBodyTransforms.TransformOnOffBodyToKvOfKeyDataPoint;
import static com.striiv.dataflow.utils.RedisUtils.*;


public class OnOffBodyPipeline extends BaseDataflowPipeline {
    public static final String EXTENDED_SENSOR_DATA_TABLE_NAME = "ExtendedSensorData";
    private final BaseDataflowPipelineOptions options;
    private final BigtableIO.Write bigtableWriter;
	public static final Logger LOG = LoggerFactory.getLogger(OnOffBodyPipeline.class);
	private static final GenericObjectPoolConfig jedisPoolConfig =  jedisPoolConfiguration(10, 100, 20000);
	private static final JedisPool jedisPool = jedisPoolInitialization(jedisPoolConfig,
			RedisConstants.GENERIC_SERVER, RedisConstants.GENERIC_SECRET);

    public static void main(String[] args) {
    	BaseDataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(BaseDataflowPipelineOptions.class);

    	BigtableIO.Write bigtableWriter = null;
    	if(options.isBigtableAccessible()) {
        	bigtableWriter = BigtableIO.write()
        			.withProjectId(options.getProject())
        			.withInstanceId("timeseries")
        			.withTableId(EXTENDED_SENSOR_DATA_TABLE_NAME);
    	}

    	new OnOffBodyPipeline(options, bigtableWriter).run();
    }

    public OnOffBodyPipeline(BaseDataflowPipelineOptions options, BigtableIO.Write bigtableWriter) {
    	this.options = options;
    	this.bigtableWriter = bigtableWriter;
    }

    private void run() {
		var paramsServiceWebAdapter = new ConfigurableParamsServiceWebAdapter(
				options.getConfigurableParamsServiceHost().get(),
				options.getConfigurableParamsServiceApiKey().get());
		var extendedSensorDataConfig = new BigtableInstanceConfig(options.getProject(),
				"timeseries", EXTENDED_SENSOR_DATA_TABLE_NAME);
		var extendedSensorDataBigtableConfig = extendedSensorDataConfig
				.getCloudBigtableScanConfiguration();

    	var pipeline = Pipeline.create(options);
    	// configurable params are pass around as JSONObject, hence the custom coder.
    	pipeline.getCoderRegistry().registerCoderForClass(JsonObject.class, JsonObjectCoder.of());

    	PCollection<UnifiedBinary.UnifiedBinaryMessage> unifiedBinaryMessage = performReadFromPubSubAndApplyFixedWindow(pipeline,
    			options.getInputTopic());

    	PCollectionList<UnifiedBinaryMessage> partitionedUnifiedBinaryMessages =
    			partitionUnifiedBinaryMessages(unifiedBinaryMessage);

    	PCollection<UnifiedBinary.UnifiedBinaryMessage> lowMotion = processLowMotionMessages(partitionedUnifiedBinaryMessages.get(
    			UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.LOW_MOTION.getNumber()));

    	PCollection<UnifiedBinary.UnifiedBinaryMessage> heartRate =
				partitionedUnifiedBinaryMessages.get(
						UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.HEART_RATE.getNumber());

    	PCollection<UnifiedBinary.UnifiedBinaryMessage> respRate =
				partitionedUnifiedBinaryMessages.get(
						UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.RESP_RATE.getNumber());

    	PCollection<UnifiedBinary.UnifiedBinaryMessage> temperature =
				partitionedUnifiedBinaryMessages.get(
						UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.TEMPERATURE.getNumber());

		PCollectionList<UnifiedBinary.UnifiedBinaryMessage> collections = PCollectionList.of(heartRate).and(respRate).and(temperature).and(lowMotion);

		PCollection<UnifiedBinary.UnifiedBinaryMessage> onOffBodyMessages = collections.apply(Flatten.pCollections());

		PCollection<KV<JsonObject, KV<DataPoint, UnifiedBinary.UnifiedBinaryEntry>>> offBodyDataPoints = onOffBodyMessages
				.apply("ToDataPointUnifiedBinaryEntryKV",
						ParDo.of(new DataPointTransforms.UnifiedBinaryMessageToDataPointUnifiedBinaryEntryKV(paramsServiceWebAdapter)));


		PCollection<KV<Timeseries.DataPoint, Map<String,Float>>> offBodyDataPointMetric =
				offBodyDataPoints
						.apply("TransformOnOffBodyToKvOfKeyDataPoint", ParDo.of(new TransformOnOffBodyToKvOfKeyDataPoint()))
						.apply("Apply3MinuteFixedWindow",
								Window.<KV<String, KV<Integer, DataPoint>>>into(
										FixedWindows.of(Duration.standardSeconds(180))).withAllowedLateness(Duration.ZERO))
						.apply("GroupByUserIdTimestamp", GroupByKey.create())
						.apply("RemoveUserIdTimestampKey", Values.create())
						.apply("DetectOnOffBodyEvent", ParDo.of(new DetectOnOffBodyEvent()))
						.apply("ProcessEachDataPointOnOffBody", ParDo.of(new RunOffBodyAlgorithm.ProcessEachDataPointOnOffBody()))
						.apply("FetchDataFromBigtable", ParDo.of(new RunOffBodyAlgorithm.FetchInputDataFromBigtable(extendedSensorDataBigtableConfig)));


		PCollection<KV<ByteString, Iterable<Mutation>>> offBodyDataMutated = offBodyDataPointMetric
				.apply("RunOffBodyAlgorithm", new OnOffBodyTransforms.RunOffBodyAlgorithm(options,
						extendedSensorDataBigtableConfig));

		PCollection<DataPoint> afterWaitOffBodyDataPoints = null;
		var offBodyDataPoint = offBodyDataPointMetric
				.apply("GetDataPoint", Keys.create());

		if(options.isBigtableAccessible()) {
			PCollection<BigtableWriteResult> writeResults = offBodyDataMutated
					.apply("WriteOffBodyMutations", bigtableWriter.withWriteResults());

			afterWaitOffBodyDataPoints = offBodyDataPoint
					.apply("WaitOnOffBodyWriteResults", Wait.on(writeResults));
		}

		afterWaitOffBodyDataPoints = afterWaitOffBodyDataPoints != null ? afterWaitOffBodyDataPoints : offBodyDataPoint;

		// Process Off Body
		processOffBodyState(afterWaitOffBodyDataPoints, options);

    	pipeline.run();
    }

    private PCollection<UnifiedBinaryMessage> processLowMotionMessages(PCollection<UnifiedBinaryMessage> lowMotionMessages) {
        PCollection<KV<ByteString, Iterable<Mutation>>> offBodyDataMutated = lowMotionMessages
        		.apply("MakeLowMotionMutations", new LowMotionTransforms.MakeLowMotionMutations());

        PCollection<UnifiedBinaryMessage> afterWaitLowMotionMessages = null;
        if(options.isBigtableAccessible()) {
        	PCollection<BigtableWriteResult> writeResults = offBodyDataMutated
        			.apply("WriteLowMotionMutations", bigtableWriter.withWriteResults());
        	afterWaitLowMotionMessages = lowMotionMessages.apply("WaitOnLowMotionWriteResults", Wait.on(writeResults));
        }

        afterWaitLowMotionMessages = afterWaitLowMotionMessages != null ? afterWaitLowMotionMessages : lowMotionMessages;

        return afterWaitLowMotionMessages;
    }


    protected static void processOffBodyState(PCollection<DataPoint> onOffBodyMessages, BaseDataflowPipelineOptions options) {
    	PCollection<PubsubMessage> offBodyPubSubMessage = onOffBodyMessages
    			.apply("ProcessOnOffBodyState", new ProcessOffBodyState(options));

    	if(options.getAggregateDataTaskTopicName().isAccessible()) {
			offBodyPubSubMessage.apply(String.format("WriteMessagesToPubSubFor%s",
					BigtableConstants.ONOFF_BODY), PubsubIO.writeMessages().to(options.getAggregateDataTaskTopicName()));

    		PDone.in(onOffBodyMessages.getPipeline());
    	}
    }

    /*
    The algorithm depends on multiple data types to discern whether the BioSticker is on or off. The method is called passing messages for each
    data type. When all data types required for the algorithm have been ingested the off body algorithm is run on them.
    This is done in two-phases, to keep with the out-of-order nature of dataflow and not to store PHI in memorystore.
    1) Keep track of data types ingested per user id per timestamp in memorystore. Emit a dataPoint when all required data types are ingested.
    2) Fetch the data types for the dataPoint from Bigtable and do the calculation.
     */
    public static class ProcessOffBodyState
    	extends PTransform<PCollection<DataPoint>, PCollection<PubsubMessage>> {

		private final transient BaseDataflowPipelineOptions options;

    	public ProcessOffBodyState(BaseDataflowPipelineOptions options) {
    		this.options = options;
    	}

    	@Override
        public PCollection<PubsubMessage> expand(PCollection<DataPoint> offBodyDataMutated) {

            return offBodyDataMutated
                    .apply(String.format("FilterFutureTimestamps%s", BigtableConstants.ONOFF_BODY),
                            ParDo.of(new FilterTransforms.FilterFutureTimestamps()))
                    .apply(String.format("FilterByElapsedTime%s", BigtableConstants.ONOFF_BODY),
                            ParDo.of(new OnOffBodyFilterByInterval(BigtableConstants.ONOFF_BODY)))
                    .apply(String.format("CreateAggregatedDataTaskPubSubMessageFor%s", BigtableConstants.ONOFF_BODY),
                    		ParDo.of(new AggregateDataTransforms.PrepareAggregateDataMessage(BigtableConstants.ONOFF_BODY)));
        }
    }

	public static class DetectOnOffBodyEvent extends DatastoreTransforms.AbstractDatastoreFn
			<Iterable<KV<Integer, DataPoint>>, Iterable<KV<Integer, DataPoint>>> {
		private final Set<Integer> expectedMembersOnOffBody = java.util.Set.of(
				UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.LOW_MOTION.getNumber(),
				UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.HEART_RATE.getNumber(),
				UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.RESP_RATE.getNumber(),
				UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.TEMPERATURE.getNumber(),
				UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.RESP_BAND_ENERGY.getNumber()
		);

		private final Set<Integer> expectedMembersLowMotion = java.util.Set.of(
				UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.LOW_MOTION.getNumber(),
				UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.HEART_RATE.getNumber(),
				UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.RESP_RATE.getNumber(),
				UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.TEMPERATURE.getNumber()
		);

		private final Set<Integer> expectedMembersRespBandEnergy = java.util.Set.of(
				UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.HEART_RATE.getNumber(),
				UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.RESP_RATE.getNumber(),
				UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.TEMPERATURE.getNumber(),
				UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.RESP_BAND_ENERGY.getNumber()
		);

		@ProcessElement
		public void processElement(ProcessContext processContext) {
			Iterable<KV<Integer, DataPoint>> dataPoints = processContext.element();

			Set<Integer> members = new HashSet<>();

			dataPoints.forEach(dataPoint -> {
					if (expectedMembersOnOffBody.contains(dataPoint.getKey())) {
						members.add(dataPoint.getKey());
					}
			});
			if (members.equals(expectedMembersLowMotion) || members.equals(expectedMembersRespBandEnergy)) {
				processContext.output(processContext.element());
			}
		}
	}

    protected static PCollectionList<UnifiedBinary.UnifiedBinaryMessage> partitionUnifiedBinaryMessages(
			PCollection<UnifiedBinary.UnifiedBinaryMessage> unifiedBinaryMessages) {
        // length + 2, because the ENTRY_DATA enum skips number 22
        return unifiedBinaryMessages.apply("PartitionByType",
                Partition.of(UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.values().length + 2,
                        new GenericFileTransforms.PartitionByEntryDataCase()));
    }

	public static class OnOffBodyFilterByInterval extends FilterTransforms.FilterByInterval {

		public OnOffBodyFilterByInterval(String dataType) {
			super(dataType);
		}

		@ProcessElement
		@Override
		public void processElement(ProcessContext processContext) {
			var dataPoint = processContext.element();
			String userId = dataPoint.getUserId();
			long timestamp = dataPoint.getTimestamp() / 1000;
			if (timestamp == 0) {
				LOG.warn("On the On-Off Body. the FilterByInterval found DataPoint with timestamp zero, corresponding to a dataType {}. No further processing done.",
						dataType);
				return;
			}
			for (int interval: intervals) {
				long roundedTimestamp = TimestampUtils.roundTimestampDown(timestamp, interval);
				var key = String.format("%s:%s:%s:%s", this.filterKeyPrefix,	userId, dataType, Long.toString(interval));
				var outputElement = false;
				try (var jedisClient = jedisPool.getResource()) {
					outputElement = wrap(() -> FilterTransforms.handleFilterByInterval(key, timestamp, roundedTimestamp, interval, jedisClient),
							LOG, jedisClient, key).get();
				} catch (redis.clients.jedis.exceptions.JedisConnectionException e) {
					LOG.error(e.getMessage(), e);
				}
				finally {
					if (outputElement) {
						processContext.output(Pair.newPair(dataPoint, interval));
					}
				}
			}
		}
	}
}