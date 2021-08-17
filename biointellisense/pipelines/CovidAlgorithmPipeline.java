package com.striiv.dataflow.algorithm_pipelines;

import com.google.api.client.util.Lists;
import com.google.gson.JsonObject;
import com.striiv.dataflow.adapters.algoparams.ConfigurableParamsServiceWebAdapter;
import com.striiv.dataflow.coders.JsonObjectCoder;
import com.striiv.dataflow.constants.BigtableConstants;
import com.striiv.dataflow.constants.RedisConstants;
import com.striiv.dataflow.transforms.CoreTransforms;
import com.striiv.dataflow.transforms.DataPointTransforms;
import com.striiv.dataflow.transforms.FilterTransforms;
import com.striiv.dataflow.utils.RedisUtils;
import com.striiv.dataflow.utils.TimestampUtils;
import com.striiv.proto.binary.UnifiedBinary;
import com.striiv.proto.core.Timeseries;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hbase.util.Pair;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.striiv.dataflow.constants.RedisConstants.*;
import static com.striiv.dataflow.utils.RedisUtils.jedisPoolConfiguration;
import static com.striiv.dataflow.utils.RedisUtils.jedisPoolInitialization;
import static java.util.stream.Collectors.toSet;

public class CovidAlgorithmPipeline {
    public static final Logger LOG = LoggerFactory.getLogger(CovidAlgorithmPipeline.class);

    private final CovidAlgorithmPipeline.CovidPredictionPipelineOptions options;
    private final ConfigurableParamsServiceWebAdapter paramsServiceWebAdapter;
    private static final GenericObjectPoolConfig jedisPoolConfig =  jedisPoolConfiguration(10, 100, 20000);
    private static final JedisPool jedisPool = jedisPoolInitialization(jedisPoolConfig,
            COVID_SERVER, RedisConstants.COVID_SECRET);

    public interface CovidPredictionPipelineOptions extends DataflowPipelineOptions {
        @Description("Pubsub topic that the pipeline will read from.  Required if the pipeline is being run in " +
                "streaming mode.  Must be a fully-qualified pubsub topic path like " +
                "`projects/project-id/topics/topic-name`.")
        ValueProvider<String> getInputTopic();
        void setInputTopic(ValueProvider<String> topic);

        @Description("Host URL of the Configurable Parameters service this pipeline should call.")
        @Validation.Required
        ValueProvider<String> getConfigurableParamsServiceHost();
        void setConfigurableParamsServiceHost(ValueProvider<String> encryptionServiceHost);

        @Description("API Key of the Configurable Parameters service this pipeline should call.")
        @Validation.Required
        ValueProvider<String> getConfigurableParamsServiceApiKey();
        void setConfigurableParamsServiceApiKey(ValueProvider<String> encryptionServiceApiKey);

        @Description("Pub/sub topic that this pipeline will publish to trigger Covid prediction pipeline." +
        		"Must be a fully-qualified pubsub topic path like " +
                "`projects/project-id/topics/topic-name`.")
        ValueProvider<String> getCovidPredictionAlgorithmTriggerTopic();
        void setCovidPredictionAlgorithmTriggerTopic(ValueProvider<String> covidPredictionAlgorithmTriggerTopic);

        @Description("Pub/sub topic that this pipeline will publish to trigger Covid classification pipeline." +
        		"Must be a fully-qualified pubsub topic path like " +
                "`projects/project-id/topics/topic-name`.")
        ValueProvider<String> getCovidClassificationAlgorithmTriggerTopic();
        void setCovidClassificationAlgorithmTriggerTopic(ValueProvider<String> covidClassificationAlgorithmTriggerTopic);
    }

    public static void main(String[] args) {
        CovidAlgorithmPipeline.CovidPredictionPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(CovidAlgorithmPipeline.CovidPredictionPipelineOptions.class);

        new CovidAlgorithmPipeline(options).run();
    }

    CovidAlgorithmPipeline(CovidAlgorithmPipeline.CovidPredictionPipelineOptions options) {
        this.options = options;
        this.paramsServiceWebAdapter = new ConfigurableParamsServiceWebAdapter(options.getConfigurableParamsServiceHost().get(),
                options.getConfigurableParamsServiceApiKey().get());
    }

    private void run() {
        var pipeline = Pipeline.create(options);

        // configurable params are pass around as JSONObject, hence the custom coder.
        pipeline.getCoderRegistry().registerCoderForClass(JsonObject.class, JsonObjectCoder.of());

        PCollection<Timeseries.DataPoint> dataPoints = pipeline.apply("ReadUnifiedBinaryMessagesFromPubSub",
            PubsubIO.readProtos(UnifiedBinary.UnifiedBinaryMessage.class).fromTopic(options.getInputTopic()))
            .apply("ToDataPointUnifiedBinaryEntryKV",
                    ParDo.of(new DataPointTransforms.UnifiedBinaryMessageToDataPointUnifiedBinaryEntryKV(paramsServiceWebAdapter)))
            .apply("MakeUserIdTimestampUnifiedBinaryEntryKV", ParDo.of(new MakeCovidUnifiedBinaryEntryKV()))
            .apply("SetSessionWindow",
                    Window.<KV<String, KV<JsonObject, KV<Timeseries.DataPoint, UnifiedBinary.UnifiedBinaryEntry>>>>into(Sessions.withGapDuration(Duration.standardMinutes(3)))
                            .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(5))))
                            .withAllowedLateness(Duration.ZERO)
                            .discardingFiredPanes())
            .apply("GroupByUserIdTimestamp", GroupByKey.create())
            .apply("RemoveUserIdTimestampKey", Values.create())
            .apply("ExtractDataPoint", ParDo.of(new ExtractDataPoint()));

        dataPoints.apply("FilterCovidPredictionTrigger",
                ParDo.of(new FilterCovidCalculationTrigger()))
                .apply("PrepareCovidPredictionTriggerMessage", ParDo.of(new PrepareCovidAlgorithmTriggerMessage()))
                .apply("TriggerCovidPredictionAlgorithm",
                        PubsubIO.writeMessages().to(options.getCovidPredictionAlgorithmTriggerTopic()));

        dataPoints.apply("FilterCovidClassificationTrigger",
                ParDo.of(new FilterTransforms.FilterByElapsedTime(BigtableConstants.COVID_CLASS,
                        RedisConstants.COVID_CLASS_ALG_PREFIX, 24*3600)))
                .apply("PrepareCovidClassificationTriggerMessage",
                        ParDo.of(new PrepareCovidAlgorithmTriggerMessage(false)))
                .apply("TriggerCovidClassificationAlgorithm",
                        PubsubIO.writeMessages().to(options.getCovidClassificationAlgorithmTriggerTopic()));

        pipeline.run();
    }

    static class MakeCovidUnifiedBinaryEntryKV extends CoreTransforms.CreateStringKV<KV<JsonObject,
            KV<Timeseries.DataPoint, UnifiedBinary.UnifiedBinaryEntry>>> {

        @Override
        public String getTag(KV<JsonObject,
                KV<Timeseries.DataPoint, UnifiedBinary.UnifiedBinaryEntry>> element) {
            var dataPoint = element.getValue().getKey();
            return String.format("%s:%s", dataPoint.getUserId(),
                    TimestampUtils.roundMillisecondTimestampToMinute(dataPoint.getTimestamp()));
        }
    }

    static class ExtractDataPoint extends DoFn<Iterable<KV<JsonObject, KV<Timeseries.DataPoint,
            UnifiedBinary.UnifiedBinaryEntry>>>, Timeseries.DataPoint> {

        @ProcessElement
        public void processElement(ProcessContext processContext) {
            List<KV<JsonObject, KV<Timeseries.DataPoint,
                    UnifiedBinary.UnifiedBinaryEntry>>> elements =
                    Lists.newArrayList(processContext.element().iterator());

            if (!elements.isEmpty()) {
                processContext.output(elements.get(0).getValue().getKey());
            }
        }
    }

    static class PrepareCovidAlgorithmTriggerMessage extends DoFn<Pair<Timeseries.DataPoint, Integer>,
            PubsubMessage> {
        boolean roundTimestamp;

        PrepareCovidAlgorithmTriggerMessage() {
            this(true);
        }

        PrepareCovidAlgorithmTriggerMessage(boolean roundTimestamp) {
            this.roundTimestamp = roundTimestamp;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            var dataPoint = c.element().getFirst();
            int roundedTimePeriod = c.element().getSecond();

            long timestamp = dataPoint.getTimestamp();

            if (this.roundTimestamp) {
                // Rounded time period is in seconds and timestamp is in milliseconds, so we must convert the rounded
                // time period to milliseconds
                timestamp = TimestampUtils.roundTimestampDown(timestamp, roundedTimePeriod * 1000);
            }
            Map<String, String> attributes = new HashMap<>();
            attributes.put("user_id", dataPoint.getUserId());
            attributes.put("timestamp", String.valueOf(timestamp));
            attributes.put("device_id", dataPoint.getDeviceId());
            attributes.put("accel_duty_cycle", String.valueOf(dataPoint.getAlgorithmFeatureSet().getAccelDutyCycle()));

            // we'll only use the attributes data field to pass to the cloud function
            // https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
            var empty = new byte[]{};
            var message = new PubsubMessage(empty, attributes);

            c.output(message);
        }
    }

    public static class FilterCovidCalculationTrigger extends DoFn<Timeseries.DataPoint,
            Pair<Timeseries.DataPoint, Integer>> {

        private List<Integer> intervals = java.util.List.of(
                3600 // hour aggregate in seconds
        );


        public  FilterCovidCalculationTrigger() {
            // The method is an intentionally-blank override.
        }

        @ProcessElement
        public void processElement(ProcessContext processContext) {

            var dataPoint = processContext.element();
            String userId = dataPoint.getUserId();
            long timestamp = dataPoint.getTimestamp() / 1000;
            if (timestamp == 0) {
                LOG.warn("FilterCovidCalculationTrigger found DataPoint with timestamp zero, " +
                        "corresponding to a dataType {}. No further processing done.", BigtableConstants.COVID_PRED);
                return;
            }
            for (int interval: intervals) {
                var filterKey = String.format("%s:%s:%s:%s", RedisConstants.COVID_PRED_ALG_PREFIX,
                        userId, BigtableConstants.COVID_PRED, interval);
                long roundedTimestamp = TimestampUtils.roundTimestampDown(timestamp, interval);
                Boolean outputElement = false;
                try (var jedis = jedisPool.getResource()) {
                    outputElement = RedisUtils.wrap(() -> handleFilterByInterval(filterKey,
                            roundedTimestamp, jedis), LOG, jedis, filterKey).get();
                }
                catch (redis.clients.jedis.exceptions.JedisConnectionException e) {
                    LOG.error(e.getMessage(), e);
                }
                finally {
                    if (null != outputElement && outputElement) {
                        processContext.output(Pair.newPair(dataPoint, interval));
                    }
                }
            }
        }

        private boolean handleFilterByInterval(String filterKey, long roundedTimestamp, Jedis jedisClient) {
            var outputData = false;
            Set<String> calculatedCovidTimestamps = jedisClient.zrange(filterKey, 0, -1);
            Set<Long> longs = calculatedCovidTimestamps.stream().map(Long::parseLong).collect(toSet());
            if (!calculatedCovidTimestamps.isEmpty()) {
                if (!longs.contains(roundedTimestamp)) {
                    jedisClient.zadd(filterKey, roundedTimestamp, Long.toString(roundedTimestamp));
                    outputData = true;
                }
            } else {
                jedisClient.zadd(filterKey, roundedTimestamp, Long.toString(roundedTimestamp));
                jedisClient.expire(filterKey, 30 * 24 * 60 * 60);
            }
            return outputData;
        }
    }

}
