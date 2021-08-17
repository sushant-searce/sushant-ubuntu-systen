package com.striiv.dataflow;

import com.striiv.dataflow.transforms.MessageLogTransforms.*;
import com.striiv.dataflow.transforms.DataPointTransforms.UnifiedBinaryMessageToDataPoint;
import com.striiv.dataflow.config.BigQueryTableConfig;
import com.striiv.dataflow.utils.RestUtils;
import com.striiv.proto.binary.*;
import com.striiv.proto.core.Timeseries;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class DeviceActivationPipeline
{
    private final DeviceActivationPipelineOptions options;

    public static void main(String[] args)
    {
        DeviceActivationPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(DeviceActivationPipelineOptions.class);

        new DeviceActivationPipeline(options).run();
    }

    DeviceActivationPipeline(DeviceActivationPipelineOptions options)
    {
        this.options = options;
    }

    public interface DeviceActivationPipelineOptions extends DataflowPipelineOptions
    {
        @Description("Full device activation endpoint URL.")
        ValueProvider<String> getActivationUrl();
        void setActivationUrl(ValueProvider<String> activationUrl);

        @Description("Full device activation handler endpoint URL.")
        ValueProvider<String> getActivationHandlerUrl();
        void setActivationHandlerUrl(ValueProvider<String> activationHandlerUrl);

        @Description("BigQuery error logging enabled flag.")
        @Default.Boolean(true)
        boolean getBigQueryEnabled();
        void setBigQueryEnabled(boolean bigQueryEnabled);

        @Description("Pub/sub topic for device activation messages to be received " +
                "This is fully-qualified topic path.")
        @Validation.Required
        String getInputTopic();
        void setInputTopic(String inputTopic);
    }

    private void run()
    {
        Pipeline pipeline = Pipeline.create(options);

        PCollection<UnifiedBinary.UnifiedBinaryMessage> messages = pipeline.apply(
                "ReadUnifiedBinaryMessagesFromPubSub",
                PubsubIO.readProtos(UnifiedBinary.UnifiedBinaryMessage.class).fromTopic(options.getInputTopic()));

        PCollection<Timeseries.DataPoint> correctedDataPoints = getCorrectedDataPoints(messages);

        postDeviceActivationAlert(correctedDataPoints);


        //TODO move the cache to after successful Netsuite request once Netsuite integration is completed
        cacheDeviceActivation(correctedDataPoints);

        PCollection<KV<Timeseries.DataPoint, RestUtils.DeviceActivationException>> failedDeviceActivationRequests =
                postDeviceActivationRequest(correctedDataPoints);

        logUndeliveredDeviceActivationRequests(failedDeviceActivationRequests);

        pipeline.run();
    }

    static PCollection<Timeseries.DataPoint> getCorrectedDataPoints(
            PCollection<UnifiedBinary.UnifiedBinaryMessage> unifiedBinaryMessages)
    {
        return unifiedBinaryMessages.apply("DeviceActivationTimeDriftCorrection",
                ParDo.of(new UnifiedBinaryMessageToDataPoint()));
    }

    private PCollection<KV<Timeseries.DataPoint, RestUtils.DeviceActivationException>> postDeviceActivationRequest(
            PCollection<Timeseries.DataPoint> messages)
    {
        return messages.apply("PostDeviceActivationRequest",
                ParDo.of(new DeviceActivationRequestEndpointCall(new RestUtils())));
    }

    private void postDeviceActivationAlert(PCollection<Timeseries.DataPoint> messages)
    {
        messages.apply("PostDeviceActivationAlert",
                ParDo.of(new DeviceActivationHandlerEndpointCall(new RestUtils())));
    }

    static void cacheDeviceActivation(PCollection<Timeseries.DataPoint> dataPoints) {
        dataPoints.apply("CacheDeviceActivationMessages",
                ParDo.of(new CacheDeviceActivation()));
    }

    private void logUndeliveredDeviceActivationRequests(
            PCollection<KV<Timeseries.DataPoint, RestUtils.DeviceActivationException>> messages)
    {
        if (this.options.getBigQueryEnabled()) {
            messages.apply("UndeliveredRequestsToTableRows", ParDo.of(
                    new MakeUndeliveredDeviceActivationTableRow()))
                    .apply("WriteUndeliveredRequestsToBigQuery", BigQueryTableConfig.createBigQueryTableConfig(
                            this.options.getProject(), "undelivered_device_activation_requests_table_schema.json")
                            .getWrite());
        }
    }
}