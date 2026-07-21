package com.flowplane.local;

import java.net.URI;
import java.time.Duration;
import java.util.function.Consumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@SpringBootApplication
public class SpringCloudStreamKafkaHttpApp {
  public static void main(String[] args) {
    SpringApplication.run(SpringCloudStreamKafkaHttpApp.class, args);
  }

  @Bean
  WebClient flowplaneRuntimeClient(@Value("${flowplane.transform-url}") URI transformUrl) {
    return WebClient.builder()
        .baseUrl(transformUrl.toString())
        .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
        .build();
  }

  @Bean
  Consumer<Message<String>> transform(WebClient flowplaneRuntimeClient, StreamBridge streamBridge) {
    return message -> {
      String sourceTopic = headerAsString(message, "kafka_receivedTopic", "spring-cloud-stream-raw");
      String responseBody;
      int statusCode;
      try {
        ResponseEntity<String> response = flowplaneRuntimeClient.post()
            .header("X-FlowPlane-Source-Topic", sourceTopic)
            .header("X-FlowPlane-Source-Key", "spring-cloud-streaming")
            .bodyValue(message.getPayload())
            .retrieve()
            .toEntity(String.class)
            .block(Duration.ofSeconds(30));
        if (response == null
            || response.getStatusCode().value() != 200
            || !"SUCCESS".equals(response.getHeaders().getFirst("X-FlowPlane-Result"))) {
          throw new IllegalStateException("Flowplane runtime returned an invalid success response.");
        }
        responseBody = response.getBody();
        statusCode = response.getStatusCode().value();
      } catch (WebClientResponseException ex) {
        statusCode = ex.getStatusCode().value();
        if (statusCode != 422 || !"ERROR".equals(ex.getHeaders().getFirst("X-FlowPlane-Result"))) {
          // Only the runtime's governed, record-level failure response belongs on
          // the data DLQ. Availability, assignment, authentication, and other
          // infrastructure failures must escape so the binder can retry them and
          // avoid committing the source offset.
          throw ex;
        }
        responseBody = ex.getResponseBodyAsString();
      }

      String bindingName = statusCode == 200 ? "transformed-out-0" : "dlq-out-0";
      Message<String> output = MessageBuilder.withPayload(responseBody == null ? "" : responseBody)
          .setHeader("flowplaneRuntimeStatus", statusCode)
          .build();
      streamBridge.send(bindingName, output);
    };
  }

  private static String headerAsString(Message<String> message, String name, String fallback) {
    Object value = message.getHeaders().get(name);
    return value == null ? fallback : value.toString();
  }
}
