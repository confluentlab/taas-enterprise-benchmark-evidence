package com.flowplane.local;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.main.Main;

public final class CamelKafkaHttpStreamingApp {
  private CamelKafkaHttpStreamingApp() {
  }

  public static void main(String[] args) throws Exception {
    String kafkaBootstrap = env("FLOWPLANE_KAFKA_BOOTSTRAP", "kafka:9092");
    String rawTopic = requiredEnv("FLOWPLANE_RAW_TOPIC");
    String transformedTopic = requiredEnv("FLOWPLANE_TRANSFORMED_TOPIC");
    String dlqTopic = requiredEnv("FLOWPLANE_DLQ_TOPIC");
    String groupId = requiredEnv("FLOWPLANE_GROUP_ID");
    String transformUrl = requiredEnv("FLOWPLANE_TRANSFORM_URL");

    Main main = new Main();
    main.configure().addRoutesBuilder(new RouteBuilder() {
      @Override
      public void configure() {
        from(kafkaConsumerUri(rawTopic, kafkaBootstrap, groupId))
            .routeId("flowplane-camel-kafka-http-streaming")
            .setHeader(Exchange.HTTP_METHOD, constant("POST"))
            .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
            .setHeader("X-FlowPlane-Source-Topic", constant(rawTopic))
            .setHeader("X-FlowPlane-Source-Key", constant("camel-kafka-streaming"))
            .toD(transformUrl + "?throwExceptionOnFailure=false")
            .choice()
              .when(header(Exchange.HTTP_RESPONSE_CODE).isEqualTo(200))
                .to(kafkaProducerUri(transformedTopic, kafkaBootstrap))
              .when(header(Exchange.HTTP_RESPONSE_CODE).isEqualTo(422))
                .to(kafkaProducerUri(dlqTopic, kafkaBootstrap))
              .otherwise()
                .log("FLOWPLANE runtime returned unexpected HTTP status ${header.CamelHttpResponseCode}");
      }
    });
    main.run();
  }

  private static String kafkaConsumerUri(String topic, String brokers, String groupId) {
    return "kafka:" + topic
        + "?brokers=" + brokers
        + "&groupId=" + groupId
        + "&autoOffsetReset=earliest";
  }

  private static String kafkaProducerUri(String topic, String brokers) {
    return "kafka:" + topic + "?brokers=" + brokers;
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
