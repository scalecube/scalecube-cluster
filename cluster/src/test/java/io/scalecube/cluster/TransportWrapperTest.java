package io.scalecube.cluster;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.net.Address;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class TransportWrapperTest {

  private final Message request =
      Message.builder()
          .sender(
              new Member(
                  "request",
                  null,
                  Collections.singletonList(Address.from("request:0")),
                  "namespace"))
          .data("" + System.currentTimeMillis())
          .build();

  private final Message response =
      Message.builder()
          .sender(
              new Member(
                  "response",
                  null,
                  Collections.singletonList(Address.from("response:0")),
                  "namespace"))
          .data("" + System.currentTimeMillis())
          .build();

  private final Transport transport;
  private final TransportWrapper transportWrapper;

  public TransportWrapperTest() {
    transport = mock(Transport.class);
    transportWrapper = new TransportWrapper(transport);
  }

  static Stream<Arguments> methodSource() {
    final Builder<Arguments> builder = Stream.builder();

    // int size, int startIndex, int successIndex

    for (int size = 0; size < 5; size++) {
      populateBuilder(builder, size);
    }

    return builder.build();
  }

  static void populateBuilder(Builder<Arguments> builder, int size) {
    // int startIndex, int successIndex

    for (int startIndex = 0; startIndex < size; startIndex++) {
      for (int successIndex = 0; successIndex < size; successIndex++) {
        builder.add(Arguments.of(size, startIndex, successIndex));
      }
    }
  }

  private Map<Member, AtomicInteger> addressIndexByMember()
      throws NoSuchFieldException, IllegalAccessException {
    final Field field = TransportWrapper.class.getDeclaredField("addressIndexByMember");
    field.setAccessible(true);
    //noinspection unchecked
    return (Map<Member, AtomicInteger>) field.get(transportWrapper);
  }

  @ParameterizedTest
  @MethodSource("methodSource")
  void requestResponseShouldWorkByRoundRobin(int size, int startIndex, int successIndex)
      throws Exception {
    final List<Address> addresses = new ArrayList<>();
    final Member member = new Member("test", null, addresses, "namespace");
    for (int i = 0; i < size; i++) {
      addresses.add(Address.from("test:" + i));
    }

    if (startIndex > 0) {
      addressIndexByMember().put(member, new AtomicInteger(startIndex));
    }

    for (int i = 0; i < size; i++) {
      final Address address = addresses.get(i);
      if (i == successIndex) {
        when(transport.requestResponse(address, request)).thenReturn(Mono.just(response));
      } else {
        when(transport.requestResponse(address, request))
            .thenReturn(Mono.error(new RuntimeException("Error - " + i)));
      }
    }

    StepVerifier.create(transportWrapper.requestResponse(member, request))
        .assertNext(message -> Assertions.assertSame(response, message, "response"))
        .thenCancel()
        .verify();
  }

  @Test
  void requestResponseShouldWorkThenFail() {
    final List<Address> addresses = Collections.singletonList(Address.from("test:0"));
    final Member member = new Member("test", null, addresses, "namespace");

    when(transport.requestResponse(addresses.get(0), request))
        .thenReturn(Mono.just(response))
        .thenReturn(Mono.error(new RuntimeException("Error")));

    StepVerifier.create(transportWrapper.requestResponse(member, request))
        .assertNext(message -> Assertions.assertSame(response, message, "response"))
        .thenCancel()
        .verify();

    StepVerifier.create(transportWrapper.requestResponse(member, request))
        .verifyErrorSatisfies(
            throwable -> Assertions.assertEquals("Error", throwable.getMessage()));
  }

  @Test
  void requestResponseShouldFailThenWork() {
    final List<Address> addresses = Collections.singletonList(Address.from("test:0"));
    final Member member = new Member("test", null, addresses, "namespace");

    when(transport.requestResponse(addresses.get(0), request))
        .thenReturn(Mono.error(new RuntimeException("Error")))
        .thenReturn(Mono.just(response));

    StepVerifier.create(transportWrapper.requestResponse(member, request))
        .verifyErrorSatisfies(
            throwable -> Assertions.assertEquals("Error", throwable.getMessage()));

    StepVerifier.create(transportWrapper.requestResponse(member, request))
        .assertNext(message -> Assertions.assertSame(response, message, "response"))
        .thenCancel()
        .verify();
  }

  @ParameterizedTest
  @MethodSource("methodSource")
  void requestResponseShouldFailByRoundRobin(int size, int startIndex, int ignore)
      throws Exception {
    final List<Address> addresses = new ArrayList<>();
    final Member member = new Member("test", null, addresses, "namespace");
    for (int i = 0; i < size; i++) {
      addresses.add(Address.from("test:" + i));
    }

    if (startIndex > 0) {
      addressIndexByMember().put(member, new AtomicInteger(startIndex));
    }

    for (int i = 0; i < size; i++) {
      final Address address = addresses.get(i);
      when(transport.requestResponse(address, request))
          .thenReturn(Mono.error(new RuntimeException("Error")));
    }

    StepVerifier.create(transportWrapper.requestResponse(member, request))
        .verifyErrorSatisfies(
            throwable -> Assertions.assertEquals("Error", throwable.getMessage()));
  }

  @ParameterizedTest
  @MethodSource("methodSource")
  void sendShouldWorkByRoundRobin(int size, int startIndex, int successIndex) throws Exception {
    final List<Address> addresses = new ArrayList<>();
    final Member member = new Member("test", null, addresses, "namespace");
    for (int i = 0; i < size; i++) {
      addresses.add(Address.from("test:" + i));
    }

    if (startIndex > 0) {
      addressIndexByMember().put(member, new AtomicInteger(startIndex));
    }

    for (int i = 0; i < size; i++) {
      final Address address = addresses.get(i);
      if (i == successIndex) {
        when(transport.send(address, request)).thenReturn(Mono.empty());
      } else {
        when(transport.send(address, request))
            .thenReturn(Mono.error(new RuntimeException("Error - " + i)));
      }
    }

    StepVerifier.create(transportWrapper.send(member, request)).verifyComplete();
  }

  @ParameterizedTest
  @MethodSource("methodSource")
  void sendShouldFailByRoundRobin(int size, int startIndex, int ignore) throws Exception {
    final List<Address> addresses = new ArrayList<>();
    final Member member = new Member("test", null, addresses, "namespace");
    for (int i = 0; i < size; i++) {
      addresses.add(Address.from("test:" + i));
    }

    if (startIndex > 0) {
      addressIndexByMember().put(member, new AtomicInteger(startIndex));
    }

    for (int i = 0; i < size; i++) {
      final Address address = addresses.get(i);
      when(transport.send(address, request)).thenReturn(Mono.error(new RuntimeException("Error")));
    }

    StepVerifier.create(transportWrapper.send(member, request))
        .verifyErrorSatisfies(
            throwable -> Assertions.assertEquals("Error", throwable.getMessage()));
  }

  @Test
  void sendShouldWorkThenFail() {
    final List<Address> addresses = Collections.singletonList(Address.from("test:0"));
    final Member member = new Member("test", null, addresses, "namespace");

    when(transport.send(addresses.get(0), request))
        .thenReturn(Mono.empty())
        .thenReturn(Mono.error(new RuntimeException("Error")));

    StepVerifier.create(transportWrapper.send(member, request)).verifyComplete();
    StepVerifier.create(transportWrapper.send(member, request))
        .verifyErrorSatisfies(
            throwable -> Assertions.assertEquals("Error", throwable.getMessage()));
  }

  @Test
  void sendShouldFailThenWork() {
    final List<Address> addresses = Collections.singletonList(Address.from("test:0"));
    final Member member = new Member("test", null, addresses, "namespace");

    when(transport.send(addresses.get(0), request))
        .thenReturn(Mono.error(new RuntimeException("Error")))
        .thenReturn(Mono.empty());

    StepVerifier.create(transportWrapper.send(member, request))
        .verifyErrorSatisfies(
            throwable -> Assertions.assertEquals("Error", throwable.getMessage()));
    StepVerifier.create(transportWrapper.send(member, request)).verifyComplete();
  }
}
