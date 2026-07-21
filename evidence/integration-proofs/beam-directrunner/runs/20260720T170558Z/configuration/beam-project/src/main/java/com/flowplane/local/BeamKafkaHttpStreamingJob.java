package com.flowplane.local;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class BeamKafkaHttpStreamingJob {
  private static final TupleTag<KV<String, String>> TRANSFORMED = new TupleTag<>() {
  };
  private static final TupleTag<KV<String, String>> DLQ = new TupleTag<>() {
  };

  private BeamKafkaHttpStreamingJob() {
  }

  public static void main(String[] args) {
    String kafkaBootstrap = env("FLOWPLANE_KAFKA_BOOTSTRAP", "kafka:9092");
    String rawTopic = requiredEnv("FLOWPLANE_RAW_TOPIC");
    String transformedTopic = requiredEnv("FLOWPLANE_TRANSFORMED_TOPIC");
    String dlqTopic = requiredEnv("FLOWPLANE_DLQ_TOPIC");
    String groupId = requiredEnv("FLOWPLANE_GROUP_ID");
    URI transformUri = URI.create(requiredEnv("FLOWPLANE_TRANSFORM_URL"));
    long maxRecords = Long.parseLong(env("FLOWPLANE_MAX_RECORDS", "2"));
    long maxReadTimeSeconds = Long.parseLong(env("FLOWPLANE_MAX_READ_TIME_SECONDS", "0"));

    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

    Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());
    KafkaIO.Read<String, String> kafkaRead = KafkaIO.<String, String>read()
            .withBootstrapServers(kafkaBootstrap)
            .withTopic(rawTopic)
            .withConsumerConfigUpdates(consumerConfig)
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withMaxNumRecords(maxRecords);

    if (maxReadTimeSeconds > 0) {
      kafkaRead = kafkaRead.withMaxReadTime(org.joda.time.Duration.standardSeconds(maxReadTimeSeconds));
    }

    PCollection<KV<String, String>> input = pipeline
        .apply("ReadRawKafka", kafkaRead.withoutMetadata());

    PCollectionTuple results = input.apply("CallFlowPlaneRuntime",
        ParDo.of(new RuntimeHttpDoFn(transformUri, rawTopic))
            .withOutputTags(TRANSFORMED, TupleTagList.of(DLQ)));

    results.get(TRANSFORMED).apply("WriteTransformedKafka", KafkaIO.<String, String>write()
        .withBootstrapServers(kafkaBootstrap)
        .withTopic(transformedTopic)
        .withKeySerializer(StringSerializer.class)
        .withValueSerializer(StringSerializer.class));

    results.get(DLQ).apply("WriteDlqKafka", KafkaIO.<String, String>write()
        .withBootstrapServers(kafkaBootstrap)
        .withTopic(dlqTopic)
        .withKeySerializer(StringSerializer.class)
        .withValueSerializer(StringSerializer.class));

    pipeline.run().waitUntilFinish();
  }

  private static final class RuntimeHttpDoFn extends DoFn<KV<String, String>, KV<String, String>> {
    private final URI transformUri;
    private final String rawTopic;
    private transient HttpClient httpClient;

    private RuntimeHttpDoFn(URI transformUri, String rawTopic) {
      this.transformUri = transformUri;
      this.rawTopic = rawTopic;
    }

    @Setup
    public void setup() {
      this.httpClient = HttpClient.newBuilder()
          .connectTimeout(Duration.ofSeconds(10))
          .build();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException, InterruptedException {
      KV<String, String> record = context.element();
      HttpRequest request = HttpRequest.newBuilder(transformUri)
          .timeout(Duration.ofSeconds(30))
          .header("Content-Type", "application/json")
          .header("X-FlowPlane-Source-Topic", rawTopic)
          .header("X-FlowPlane-Source-Key", "beam-kafka-streaming")
          .POST(HttpRequest.BodyPublishers.ofString(record.getValue()))
          .build();

      String recordKey = record.getKey() == null ? "beam-kafka-streaming" : record.getKey();
      HttpResponse<String> response;
      try {
        response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      } catch (IOException error) {
        context.output(DLQ, KV.of(recordKey, transportError(recordKey, error)));
        return;
      }
      KV<String, String> output = KV.of(recordKey, response.body());
      if (response.statusCode() == 200) {
        context.output(TRANSFORMED, output);
      } else if (response.statusCode() == 422) {
        context.output(DLQ, output);
      } else {
        throw new IllegalStateException("Unexpected FLOWPLANE runtime HTTP status: " + response.statusCode());
      }
    }
  }

  private static String transportError(String recordId, Exception error) {
    return "{"
        + "\"schemaVersion\":\"flowplane.runtime.error.v1\","
        + "\"code\":\"RUNTIME_TRANSPORT_ERROR\","
        + "\"error\":{"
        + "\"code\":\"RUNTIME_TRANSPORT_ERROR\","
        + "\"message\":\"" + jsonEscape(error.getMessage()) + "\","
        + "\"stage\":\"BEAM_HTTP_CLIENT\","
        + "\"retryable\":true"
        + "},"
        + "\"dlq\":{"
        + "\"reason\":\"RUNTIME_TRANSPORT_ERROR\","
        + "\"runtimeId\":\"beam-kafka-http-streaming\","
        + "\"originalRecordId\":\"" + jsonEscape(recordId) + "\""
        + "},"
        + "\"recordId\":\"" + jsonEscape(recordId) + "\""
        + "}";
  }

  private static String jsonEscape(String value) {
    if (value == null) {
      return "";
    }
    return value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\r", "\\r")
        .replace("\n", "\\n");
  }

  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Missing required environment variable: " + name);
    }
    return value;
  }

  private static String env(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : value;
  }
}
