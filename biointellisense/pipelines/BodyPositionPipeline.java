package com.striiv.dataflow;

import com.google.cloud.bigtable.beam.AbstractCloudBigtableTableDoFn;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import com.google.common.primitives.Ints;
import com.google.gson.JsonObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.striiv.dataflow.adapters.algoparams.ConfigurableParamsServiceWebAdapter;
import com.striiv.dataflow.coders.JsonObjectCoder;
import com.striiv.dataflow.config.BigtableInstanceConfig;
import com.striiv.dataflow.constants.BigtableConstants;
import com.striiv.dataflow.exceptions.InvalidBodyPosition;
import com.striiv.dataflow.transforms.DataPointTransforms;
import com.striiv.dataflow.transforms.GenericFileTransforms;
import com.striiv.dataflow.transforms.extended_sensor_data_transforms.ExtendedSensorDataTransforms;
import com.striiv.dataflow.transforms.extended_sensor_data_transforms.InclineAngleBinTransforms;
import com.striiv.dataflow.utils.PostureUtils;
import com.striiv.dataflow.utils.ProtobufUtils;
import com.striiv.proto.binary.BodyPosition;
import com.striiv.proto.binary.UnifiedBinary;
import com.striiv.proto.core.Timeseries;
import com.striiv.proto.extended_sensor_data.BodyPositionCalibrationMessage;
import com.striiv.proto.extended_sensor_data.BodyPositionMessage;
import com.striiv.proto.extended_sensor_data.ExtendedSensorDataEnvelopeMessage;
import com.striiv.proto.extended_sensor_data.ExtendedSensorDataEnvelopeMessage.ExtendedSensorDataEnvelope.DataTypeCase;
import com.striiv.proto.extended_sensor_data.OrientationVectorMessage;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.math3.linear.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static com.striiv.dataflow.constants.BigtableConstants.getBytes;
import static com.striiv.dataflow.constants.BodyPositionAlgorithmConstants.*;
import static com.striiv.dataflow.transforms.AggregateDataTransforms.publishDataMessages;
import static com.striiv.dataflow.transforms.extended_sensor_data_transforms.ExtendedSensorDataTransforms.getExtendedSensorDataEnvelopeMessageBuilder;
import static java.lang.Math.*;

public class BodyPositionPipeline extends BaseDataflowPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(BodyPositionPipeline.class);
	static final Map<DataTypeCase, String>	metricMap;
	static {
		Map<DataTypeCase, String> iniMetricMap = new EnumMap<>(DataTypeCase.class);
		iniMetricMap.put(DataTypeCase.BODY_POSITION_CALIBRATION, "BODY_POS_CAL");
		iniMetricMap.put(DataTypeCase.BODY_POSITION,	"BODY_POS");
		iniMetricMap.put(DataTypeCase.ACTIVITY, "ACTIVITY");
		iniMetricMap.put(DataTypeCase.SLEEP,	"SLEEP");
		iniMetricMap.put(DataTypeCase.INCLINE_ANGLE_BIN,	"INC_ANG_BIN");
		iniMetricMap.put(DataTypeCase.LOW_MOTION, "LOW_MOTION");
		iniMetricMap.put(DataTypeCase.ON_OFF_BODY, "ONOFF_BODY");
		iniMetricMap.put(DataTypeCase.CONTACT_TRACE, "CONT_TRACE");
		iniMetricMap.put(DataTypeCase.RESP_BAND_ENERGY, "RESP_BAND_ENERGY");
		iniMetricMap.put(DataTypeCase.STEP_INTERVAL, "STEP_I");
		iniMetricMap.put(DataTypeCase.STEPS, "STEPS");
		metricMap = Collections.unmodifiableMap(iniMetricMap);
	}

	public static final String EXTENDED_SENSOR_DATA_TABLE_NAME = "ExtendedSensorData";
    private final BaseDataflowPipelineOptions options;

	static final TupleTag<KV<UnifiedBinary.UnifiedBinaryMessage, KV<Timeseries.DataPoint, Float>>> inclineAngles =
			new TupleTag<KV<UnifiedBinary.UnifiedBinaryMessage, KV<Timeseries.DataPoint, Float>>>() {
			};

	static final TupleTag<KV<UnifiedBinary.UnifiedBinaryMessage, KV<Timeseries.DataPoint, BodyPositionMessage.BodyPosition>>>
			bodyPositionDataPoints =
			new TupleTag<KV<UnifiedBinary.UnifiedBinaryMessage, KV<Timeseries.DataPoint, BodyPositionMessage.BodyPosition>>>() {
			};

    public static void main(String[] args) {
    	BaseDataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(BaseDataflowPipelineOptions.class);

    	new BodyPositionPipeline(options).run();
    }
    
    public BodyPositionPipeline(BaseDataflowPipelineOptions options) {
    	this.options = options;
    }
    
    private void run() {
    	var pipeline = Pipeline.create(options);
    	// configurable params are pass around as JSONObject, hence the custom coder.
    	pipeline.getCoderRegistry().registerCoderForClass(JsonObject.class, JsonObjectCoder.of());
    	
    	PCollection<UnifiedBinary.UnifiedBinaryMessage> unifiedBinaryMessage = performReadFromPubSubAndApplyFixedWindow(pipeline,
    			options.getInputTopic());
    	
    	PCollectionList<UnifiedBinary.UnifiedBinaryMessage> partitionedUnifiedBinaryMessages =
    			partitionUnifiedBinaryMessages(unifiedBinaryMessage);
    	
    	processBodyPositionMessages(partitionedUnifiedBinaryMessages.get(
    			UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.BODY_POSITION.getNumber()));
    	
    	pipeline.run();
    }
    
    private void processBodyPositionMessages(PCollection<UnifiedBinary.UnifiedBinaryMessage> bodyPositionMessages) {

    	var paramsServiceWebAdapter = new ConfigurableParamsServiceWebAdapter(
    			options.getConfigurableParamsServiceHost().get(), options.getConfigurableParamsServiceApiKey().get());

    	PCollection<KV<UnifiedBinary.UnifiedBinaryMessage, KV<Timeseries.DataPoint, BodyPosition.BodyPositionEntry>>> bodyPositionKvs =
				bodyPositionMessages.apply("ConvertToDataPointBodyPositionKV",
    					new UnifiedBinaryMessageToDataPointBodyPositionKV(paramsServiceWebAdapter));

		var bodyPositionAndInclineAngles =
				bodyPositionKvs.apply("MakeBodyPositionMessage",
				ParDo.of(new MakeBodyPositionMessage(
						new BigtableInstanceConfig(
								options.getProject(), "timeseries", EXTENDED_SENSOR_DATA_TABLE_NAME)
								.getCloudBigtableScanConfiguration()
				)).withOutputTags(bodyPositionDataPoints, TupleTagList.of(inclineAngles)));

		var inclineAngleBinMutations = bodyPositionAndInclineAngles
				.get(inclineAngles)
				.apply("GetKVBodyPositionInclineAngleBin", Values.create())
				.apply("RunInclineAngleBin",
						new InclineAngleBinTransforms.RunInclineAngleClassification(options.isStreaming()));

		var bodyPositionMutations = bodyPositionAndInclineAngles
				.get(bodyPositionDataPoints)
				.apply("GetKVBodyPositionDataPoint", Values.create())
				.apply("MakeExtendedSensorDataEnvelope",
						ParDo.of(new MakeExtendedSensorDataEnvelopeMessage()))
				.apply("MakeBodyPositionMutation",
				new ExtendedSensorDataTransforms.MakeExtendedSensorDataMutation(Clock.systemUTC(), options.isStreaming()));

		var mutations =
				PCollectionList.of(inclineAngleBinMutations).and(bodyPositionMutations)
				.apply("MergeInclineAngleBinAndBodyPositionMutations", Flatten.pCollections());

		var messages = bodyPositionAndInclineAngles
			.get(bodyPositionDataPoints)
			.apply("GetBodyPositionMessage",	Keys.create())
			.apply("Distinct Messages", Distinct.create());

    	if(options.isBigtableAccessible()) {
    		var bigtableWriter = BigtableIO.write()
    				.withProjectId(options.getProject())
    				.withInstanceId("timeseries")
    				.withTableId(EXTENDED_SENSOR_DATA_TABLE_NAME);

			PCollection<BigtableWriteResult> writeResults =
					mutations.apply("WriteBodyPositionMutations", bigtableWriter.withWriteResults());

			if (options.getAggregateDataTaskTopicName().isAccessible()) {
				publishDataMessages(BigtableConstants.BODY_POS, messages, writeResults, null,
						options.getAggregateDataTaskTopicName());
			}
    	}
    }
    
    static PCollectionList<UnifiedBinary.UnifiedBinaryMessage> partitionUnifiedBinaryMessages(
			PCollection<UnifiedBinary.UnifiedBinaryMessage> unifiedBinaryMessages) {
        // length + 2, because the ENTRY_DATA enum skips number 22
        return unifiedBinaryMessages.apply("PartitionByType",
                Partition.of(UnifiedBinary.UnifiedBinaryEntry.EntryDataCase.values().length + 2,
                        new GenericFileTransforms.PartitionByEntryDataCase()));
    }

	public static class MakeBodyPositionMessage extends AbstractCloudBigtableTableDoFn<
			KV<UnifiedBinary.UnifiedBinaryMessage, KV<Timeseries.DataPoint, BodyPosition.BodyPositionEntry>>,
			KV<UnifiedBinary.UnifiedBinaryMessage, KV<Timeseries.DataPoint, BodyPositionMessage.BodyPosition>>> {

		public MakeBodyPositionMessage(CloudBigtableScanConfiguration extendedSensorDataConfig) {
			super(extendedSensorDataConfig);
		}

		@ProcessElement
		public void processElement(ProcessContext processContext) {
			var dataPoint = processContext.element().getValue().getKey();
			var bodyPositionEntry = processContext.element().getValue().getValue();

			try {
				var bodyPosition = getBodyPosition(dataPoint, bodyPositionEntry);
				processContext.output(KV.of(processContext.element().getKey(), KV.of(dataPoint, bodyPosition)));
				processContext.output(inclineAngles, KV.of(processContext.element().getKey(),
						KV.of(dataPoint, bodyPosition.getInclineAngle())));
			} catch (InvalidBodyPosition ex) {
				LOG.warn(ex.getMessage());
			} catch (IOException ex) {
				LOG.error(ex.getMessage());
			}
		}

		private BodyPositionMessage.BodyPosition getBodyPosition(Timeseries.DataPoint dataPoint,
																 BodyPosition.BodyPositionEntry bodyPositionEntry)
				throws IOException, InvalidBodyPosition {
			int meanAngleDeviation = bodyPositionEntry.getVariance();
			validateMeanAngleDeviation(meanAngleDeviation);

			OrientationVectorMessage.OrientationVector walkingVector = getLastCalibration(dataPoint);
			double[] walkingVectorRow = {(double) walkingVector.getX(), (double) -walkingVector.getY(),
					(double) walkingVector.getZ()};
			RealVector walkingRealVector = new ArrayRealVector(walkingVectorRow);
			RealVector scaledWalkingVector = walkingRealVector.mapMultiplyToSelf(1d / walkingRealVector.getNorm())
					.mapMultiplyToSelf(GRAVITY_INT);

			double xRotationAngle = Math.asin(-scaledWalkingVector.getEntry(2) / GRAVITY_INT);
			double zRotationAngle = toRadians(-180) - atan2(scaledWalkingVector.getEntry(0),
					scaledWalkingVector.getEntry(1));
			RealMatrix xRotationMatrix = calculateXRotationMatrix(xRotationAngle);
			RealMatrix zRotationMatrix = calculateZRotationMatrix(zRotationAngle);


			double[] uncalibratedVectorRow = {(double) bodyPositionEntry.getGravity().getX(),
					(double) -bodyPositionEntry.getGravity().getY(), (double) bodyPositionEntry.getGravity().getZ
					()};
			RealVector uncalibratedVector = new ArrayRealVector(uncalibratedVectorRow);

			RealMatrix inverseXRotationMatrix = MatrixUtils.inverse(xRotationMatrix);
			RealMatrix inverseZRotationMatrix = MatrixUtils.inverse(zRotationMatrix);
			RealVector autoCalibratedVector = inverseXRotationMatrix.operate(inverseZRotationMatrix.operate(uncalibratedVector));

			double inclineAngle = PostureUtils.getInclineAngle(autoCalibratedVector.getEntry(1));
			double rotationAngle = PostureUtils.getRotationAngle(autoCalibratedVector.getEntry(0),
					autoCalibratedVector.getEntry(2), inclineAngle, ROTATION_ANGLE_INCLINE_THRESHOLD);

			var bodyPositionClassification =
					PostureUtils.classifyBodyPosition(inclineAngle, rotationAngle);

			return BodyPositionMessage.BodyPosition.newBuilder()
					.setVersion(ProtobufUtils.getMessageVersion(1, 0, 0))
					.setAlgoVersion(ProtobufUtils.getMessageVersion(1, 0, 0))
					.setInclineAngle((float) inclineAngle)
					.setRotationAngle((float) rotationAngle)
					.setVariance(meanAngleDeviation)
					.setConfidence(PostureUtils.meanAngleDeviationToConfidence(meanAngleDeviation))
					.setGravity(OrientationVectorMessage.OrientationVector.newBuilder()
							.setX(bodyPositionEntry.getGravity().getX())
							.setY(bodyPositionEntry.getGravity().getY())
							.setZ(bodyPositionEntry.getGravity().getZ())
							.build())
					.setBodyPosition(bodyPositionClassification)
					.setPeriod(60000)
					.build();
		}

		private OrientationVectorMessage.OrientationVector getLastCalibration(Timeseries.DataPoint dataPoint)
				throws IOException, InvalidBodyPosition {
			String userId = dataPoint.getUserId();
			String metric =
					metricMap.get(DataTypeCase
							.BODY_POSITION_CALIBRATION);
			long endTimestamp = dataPoint.getTimestamp();
			long startTimestamp = endTimestamp - MAX_MILLISECONDS_SINCE_CALIBRATION;

			byte[] startRowKey = getBytes(String.format("%s:%s:%d", userId, metric, startTimestamp));
			byte[] endRowKey = getBytes(String.format("%s:%s:%d", userId, metric, endTimestamp));

			ResultScanner extendedSensorDataResults = getLastCalibrationResults(startRowKey, endRowKey);
			var bodyPositionCalibration =
					getLastBodyPositionCalibrationFromResults(extendedSensorDataResults);

			if (bodyPositionCalibration == null) {
				throw new InvalidBodyPosition(String.format("No body position calibration occurred in the " +
								"last %d milliseconds or there weren't at least %d sample counts.",
						MAX_MILLISECONDS_SINCE_CALIBRATION, MINIMUM_WALKING_TIME_FOR_AUTOCALIBRATION));
			}

			return bodyPositionCalibration.getWalkingVector();
		}

		private static RealMatrix calculateXRotationMatrix(double rotationAngle) {
			var rotationMatrix = new double[3][3];
			double[] row0 = {1d, 0d, 0d};
			double[] row1 = {0d, cos(rotationAngle), -sin(rotationAngle)};
			double[] row2 = {0d, sin(rotationAngle), cos(rotationAngle)};
			rotationMatrix[0] = row0;
			rotationMatrix[1] = row1;
			rotationMatrix[2] = row2;

			return new Array2DRowRealMatrix(rotationMatrix);
		}

		private static RealMatrix calculateZRotationMatrix(double rotationAngle) {
			var rotationMatrix = new double[3][3];
			double[] row0 = {cos(rotationAngle), -sin(rotationAngle), 0d};
			double[] row1 = {sin(rotationAngle), cos(rotationAngle), 0d};
			double[] row2 = {0d, 0d, 1d};
			rotationMatrix[0] = row0;
			rotationMatrix[1] = row1;
			rotationMatrix[2] = row2;

			return new Array2DRowRealMatrix(rotationMatrix);
		}

		private ResultScanner getLastCalibrationResults(byte[] startRowKey, byte[] endRowKey) throws IOException {
			var extendedSensorDataTable = getConnection().getTable(TableName.valueOf(
					EXTENDED_SENSOR_DATA_TABLE_NAME));
			var extendedSensorDataScanner = new Scan();
			extendedSensorDataScanner.setStartRow(startRowKey);
			extendedSensorDataScanner.setStopRow(endRowKey);
			return extendedSensorDataTable.getScanner(extendedSensorDataScanner);
		}

		private BodyPositionCalibrationMessage.BodyPositionCalibration
		getLastBodyPositionCalibrationFromResults(ResultScanner extendedSensorDataResults)
				throws InvalidProtocolBufferException {
			BodyPositionCalibrationMessage.BodyPositionCalibration lastBodyPositionCalibration = null;
			long maxTimestamp = -1;
			for (Result row : extendedSensorDataResults) {
				byte[] rowKey = row.getRow();
				var timestampBase = Long.parseLong(Bytes.toString(rowKey).split(":")[2]);
				List<Cell> cells = row.listCells();

				for (Cell cell : cells) {
					var timestampOffset = Ints.fromByteArray(cell.getQualifierArray());
					long timestamp = timestampBase + timestampOffset;

					var extendedSensorDataEnvelope =
							ExtendedSensorDataEnvelopeMessage.ExtendedSensorDataEnvelope.parseFrom(cell.getValueArray());

					var bodyPositionCalibration =
							extendedSensorDataEnvelope.getBodyPositionCalibration();
					if (timestamp > maxTimestamp &&
							bodyPositionCalibration.getSampleCount() >= MINIMUM_WALKING_TIME_FOR_AUTOCALIBRATION) {
						maxTimestamp = timestamp;
						lastBodyPositionCalibration = bodyPositionCalibration;
					}
				}
			}
			return lastBodyPositionCalibration;
		}

		private void validateMeanAngleDeviation(int meanAngleDeviation) throws InvalidBodyPosition {
			if (meanAngleDeviation > ANGLE_DEVIATION_THRESHOLD) {
				throw new InvalidBodyPosition(String.format("Mean angle deviation: %d is greater than maximum" +
						"threshold: %d", meanAngleDeviation, ANGLE_DEVIATION_THRESHOLD));
			}
		}
	}

	public static class UnifiedBinaryMessageToDataPointBodyPositionKV extends PTransform<
			PCollection<UnifiedBinary.UnifiedBinaryMessage>,
			PCollection<KV<UnifiedBinary.UnifiedBinaryMessage, KV<Timeseries.DataPoint, BodyPosition.BodyPositionEntry>>>> {

		private ConfigurableParamsServiceWebAdapter paramsServiceWebAdapter;

		public UnifiedBinaryMessageToDataPointBodyPositionKV(ConfigurableParamsServiceWebAdapter paramsServiceWebAdapter) {
			this.paramsServiceWebAdapter = paramsServiceWebAdapter;
		}

		@Override
		public PCollection<KV<UnifiedBinary.UnifiedBinaryMessage, KV<Timeseries.DataPoint, BodyPosition.BodyPositionEntry>>> expand(
				PCollection<UnifiedBinary.UnifiedBinaryMessage> input) {
			return input.apply("UnifiedBinaryMessageAndDataPointUnifiedBinaryEntryKV", ParDo.of(
					new DataPointTransforms.UnifiedBinaryMessageAndDataPointUnifiedBinaryEntryKV(paramsServiceWebAdapter)))
					.apply("UnifiedBinaryEntryKVToBodyPositionEntryKV", ParDo.of(
							new UnifiedBinaryMessageToDataPointBodyPositionKV.
									UnifiedBinaryEntryKToBodyPositionEntryKV<>()));
		}

		private static class UnifiedBinaryEntryKToBodyPositionEntryKV<K> extends DoFn<
				KV<UnifiedBinary.UnifiedBinaryMessage, KV<JsonObject, KV<K, UnifiedBinary.UnifiedBinaryEntry>>>,
				KV<UnifiedBinary.UnifiedBinaryMessage, KV<K, BodyPosition.BodyPositionEntry>>> {

			@ProcessElement
			public void processElement(ProcessContext processContext) {
				KV<K, BodyPosition.BodyPositionEntry> bodyPositionEntryKV = KV.of(
						processContext.element().getValue().getValue().getKey(),
						processContext.element().getValue().getValue().getValue().getBodyPosition()
				);
				processContext.output(KV.of(processContext.element().getKey() ,bodyPositionEntryKV));
			}
		}
	}

	public static class MakeExtendedSensorDataEnvelopeMessage extends DoFn<KV<Timeseries.DataPoint,
			BodyPositionMessage.BodyPosition>, KV<Timeseries.DataPoint, ExtendedSensorDataEnvelopeMessage
			.ExtendedSensorDataEnvelope>> {

		@ProcessElement
		public void processElement(ProcessContext processContext) {
			var extendedSensorDataEnvelope =
					getExtendedSensorDataEnvelopeMessageBuilder()
							.setBodyPosition(processContext.element().getValue())
							.build();

			processContext.output(KV.of(processContext.element().getKey(), extendedSensorDataEnvelope));
		}
	}
}