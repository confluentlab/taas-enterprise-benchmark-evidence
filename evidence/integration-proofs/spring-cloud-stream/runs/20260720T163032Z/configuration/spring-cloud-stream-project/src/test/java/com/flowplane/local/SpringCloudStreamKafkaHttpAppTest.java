package com.flowplane.local;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.function.Consumer;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

class SpringCloudStreamKafkaHttpAppTest {
  private MockWebServer runtime;
  private StreamBridge streamBridge;
  private Consumer<Message<String>> processor;

  @BeforeEach
  void setUp() throws Exception {
    runtime = new MockWebServer();
    runtime.start();
    streamBridge = mock(StreamBridge.class);
    WebClient client = WebClient.builder().baseUrl(runtime.url("/transform").toString()).build();
    processor = new SpringCloudStreamKafkaHttpApp().transform(client, streamBridge);
  }

  @AfterEach
  void tearDown() throws Exception {
    runtime.shutdown();
  }

  @Test
  void publishesSuccessfulRuntimeResponseToTransformedTopic() {
    runtime.enqueue(jsonResponse(200, "{\"status\":\"SUCCESS\",\"value\":{\"id\":1}}"));

    processor.accept(sourceMessage());

    verify(streamBridge).send(eq("transformed-out-0"), argThat(output ->
        output instanceof Message<?> message && message.getPayload().toString().contains("SUCCESS")));
    verify(streamBridge, never()).send(eq("dlq-out-0"), org.mockito.ArgumentMatchers.any());
  }

  @Test
  void publishesRuntimeBuiltPolicyEnvelopeToDlqWithoutRebuildingIt() {
    String envelope = "{\"status\":\"ERROR\",\"dlq\":{\"mappingId\":\"orders\",\"reason\":\"NULL_POLICY\"}}";
    runtime.enqueue(jsonResponse(422, envelope));

    processor.accept(sourceMessage());

    verify(streamBridge).send(eq("dlq-out-0"), argThat(output ->
        output instanceof Message<?> message && envelope.equals(message.getPayload())));
    verify(streamBridge, never()).send(eq("transformed-out-0"), org.mockito.ArgumentMatchers.any());
  }

  @Test
  void propagatesUnavailableRuntimeForBinderRetryInsteadOfWritingDlq() {
    runtime.enqueue(jsonResponse(503, "{\"status\":\"NO_ASSIGNMENT\"}"));

    assertThrows(WebClientResponseException.ServiceUnavailable.class,
        () -> processor.accept(sourceMessage()));

    verify(streamBridge, never()).send(eq("dlq-out-0"), org.mockito.ArgumentMatchers.any());
    verify(streamBridge, never()).send(eq("transformed-out-0"), org.mockito.ArgumentMatchers.any());
  }

  @Test
  void propagatesUnexpectedClientErrorInsteadOfMisclassifyingItAsRecordDlq() {
    runtime.enqueue(jsonResponse(401, "{\"error\":\"unauthorized\"}"));

    assertThrows(WebClientResponseException.Unauthorized.class,
        () -> processor.accept(sourceMessage()));

    verify(streamBridge, never()).send(eq("dlq-out-0"), org.mockito.ArgumentMatchers.any());
  }

  @Test
  void rejectsUnmarked422InsteadOfMisclassifyingItAsGovernedDlq() {
    runtime.enqueue(new MockResponse()
        .setResponseCode(422)
        .setHeader("Content-Type", "application/json")
        .setBody("{\"error\":\"proxy rejection\"}"));

    assertThrows(WebClientResponseException.UnprocessableEntity.class,
        () -> processor.accept(sourceMessage()));

    verify(streamBridge, never()).send(eq("dlq-out-0"), org.mockito.ArgumentMatchers.any());
    verify(streamBridge, never()).send(eq("transformed-out-0"), org.mockito.ArgumentMatchers.any());
  }

  private Message<String> sourceMessage() {
    return MessageBuilder.withPayload("{\"id\":1}")
        .setHeader("kafka_receivedTopic", "flowplane.demo.orders.raw")
        .build();
  }

  private MockResponse jsonResponse(int status, String body) {
    MockResponse response = new MockResponse()
        .setResponseCode(status)
        .setHeader("Content-Type", "application/json")
        .setBody(body);
    if (status == 200) {
      response.setHeader("X-FlowPlane-Result", "SUCCESS");
    } else if (status == 422) {
      response.setHeader("X-FlowPlane-Result", "ERROR");
    }
    return response;
  }
}
