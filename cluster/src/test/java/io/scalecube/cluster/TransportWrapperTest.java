package io.scalecube.cluster;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.net.Address;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
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

  @Nested
  class RequestResponseTests {

    @Test
    void requestResponseShouldWork() {
      final List<Address> addresses = Collections.singletonList(Address.from("test:0"));
      final Member member = new Member("test", null, addresses, "namespace");
      final Transport transport = mock(Transport.class);
      final TransportWrapper transportWrapper = new TransportWrapper(transport);

      when(transport.requestResponse(addresses.get(0), request)).thenReturn(Mono.just(response));

      StepVerifier.create(transportWrapper.requestResponse(member, request).retry(2))
          .assertNext(message -> Assertions.assertSame(response, message, "response"))
          .thenCancel()
          .verify();
    }

    @Test
    void requestResponseShouldWorkMemberSingleAddress() {
      final List<Address> addresses = Collections.singletonList(Address.from("test:0"));
      final Member member = new Member("test", null, addresses, "namespace");
      final Transport transport = mock(Transport.class);
      final TransportWrapper transportWrapper = new TransportWrapper(transport);

      when(transport.requestResponse(addresses.get(0), request)).thenReturn(Mono.just(response));

      StepVerifier.create(transportWrapper.requestResponse(member, request).retry(2))
          .assertNext(message -> Assertions.assertSame(response, message, "response"))
          .thenCancel()
          .verify();
    }

    @Test
    void requestResponseShouldWorkMemberTwoAddresses() {
      final List<Address> addresses = Arrays.asList(Address.from("test:0"), Address.from("test:1"));
      final Member member = new Member("test", null, addresses, "namespace");
      final Transport transport = mock(Transport.class);
      final TransportWrapper transportWrapper = new TransportWrapper(transport);

      when(transport.requestResponse(addresses.get(0), request))
          .thenReturn(Mono.error(new RuntimeException("Error")));
      when(transport.requestResponse(addresses.get(1), request)).thenReturn(Mono.just(response));

      StepVerifier.create(transportWrapper.requestResponse(member, request).retry(2))
          .assertNext(message -> Assertions.assertSame(response, message, "response"))
          .thenCancel()
          .verify();
    }

    @Test
    void requestResponseShouldWorkMemberThreeAddresses() {
      final List<Address> addresses =
          Arrays.asList(Address.from("test:0"), Address.from("test:1"), Address.from("test:2"));
      final Member member = new Member("test", null, addresses, "namespace");
      final Transport transport = mock(Transport.class);
      final TransportWrapper transportWrapper = new TransportWrapper(transport);

      when(transport.requestResponse(addresses.get(0), request))
          .thenReturn(Mono.error(new RuntimeException("Error")));
      when(transport.requestResponse(addresses.get(1), request))
          .thenReturn(Mono.error(new RuntimeException("Error")));
      when(transport.requestResponse(addresses.get(2), request)).thenReturn(Mono.just(response));

      StepVerifier.create(transportWrapper.requestResponse(member, request).retry(2))
          .assertNext(message -> Assertions.assertSame(response, message, "response"))
          .thenCancel()
          .verify();
    }

    @Test
    void requestResponseShouldFailMemberSingleAddress() {
      final List<Address> addresses = Collections.singletonList(Address.from("test:0"));
      final Member member = new Member("test", null, addresses, "namespace");
      final Transport transport = mock(Transport.class);
      final TransportWrapper transportWrapper = new TransportWrapper(transport);

      when(transport.requestResponse(addresses.get(0), request))
          .thenReturn(Mono.error(new RuntimeException("Error")));

      StepVerifier.create(transportWrapper.requestResponse(member, request).retry(2))
          .verifyErrorSatisfies(
              throwable -> Assertions.assertEquals("Error", throwable.getMessage()));
    }

    @Test
    void requestResponseShouldFailMemberTwoAddresses() {
      final List<Address> addresses = Arrays.asList(Address.from("test:0"), Address.from("test:1"));
      final Member member = new Member("test", null, addresses, "namespace");
      final Transport transport = mock(Transport.class);
      final TransportWrapper transportWrapper = new TransportWrapper(transport);

      when(transport.requestResponse(addresses.get(0), request))
          .thenReturn(Mono.error(new RuntimeException("Error - 0")));
      when(transport.requestResponse(addresses.get(1), request))
          .thenReturn(Mono.error(new RuntimeException("Error - 1")));

      StepVerifier.create(transportWrapper.requestResponse(member, request).retry(2))
          .verifyErrorSatisfies(
              throwable -> Assertions.assertEquals("Error - 1", throwable.getMessage()));
    }

    @Test
    void requestResponseShouldFailMemberThreeAddresses() {
      final List<Address> addresses =
          Arrays.asList(Address.from("test:0"), Address.from("test:1"), Address.from("test:2"));
      final Member member = new Member("test", null, addresses, "namespace");
      final Transport transport = mock(Transport.class);
      final TransportWrapper transportWrapper = new TransportWrapper(transport);

      when(transport.requestResponse(addresses.get(0), request))
          .thenReturn(Mono.error(new RuntimeException("Error - 0")));
      when(transport.requestResponse(addresses.get(1), request))
          .thenReturn(Mono.error(new RuntimeException("Error - 1")));
      when(transport.requestResponse(addresses.get(2), request))
          .thenReturn(Mono.error(new RuntimeException("Error - 2")));

      StepVerifier.create(transportWrapper.requestResponse(member, request).retry(2))
          .verifyErrorSatisfies(
              throwable -> Assertions.assertEquals("Error - 2", throwable.getMessage()));
    }
  }

  @Nested
  class SendTests {

    @Test
    void sendShouldWork() {
      final List<Address> addresses = Collections.singletonList(Address.from("test:0"));
      final Member member = new Member("test", null, addresses, "namespace");
      final Transport transport = mock(Transport.class);
      final TransportWrapper transportWrapper = new TransportWrapper(transport);

      when(transport.send(addresses.get(0), request)).thenReturn(Mono.empty());

      StepVerifier.create(transportWrapper.send(member, request).retry(2)).verifyComplete();
    }

    @Test
    void sendShouldWorkMemberSingleAddress() {
      final List<Address> addresses = Collections.singletonList(Address.from("test:0"));
      final Member member = new Member("test", null, addresses, "namespace");
      final Transport transport = mock(Transport.class);
      final TransportWrapper transportWrapper = new TransportWrapper(transport);

      when(transport.send(addresses.get(0), request)).thenReturn(Mono.empty());

      StepVerifier.create(transportWrapper.send(member, request).retry(2)).verifyComplete();
    }

    @Test
    void sendShouldWorkMemberTwoAddresses() {
      final List<Address> addresses = Arrays.asList(Address.from("test:0"), Address.from("test:1"));
      final Member member = new Member("test", null, addresses, "namespace");
      final Transport transport = mock(Transport.class);
      final TransportWrapper transportWrapper = new TransportWrapper(transport);

      when(transport.send(addresses.get(0), request))
          .thenReturn(Mono.error(new RuntimeException("Error")));
      when(transport.send(addresses.get(1), request)).thenReturn(Mono.empty());

      StepVerifier.create(transportWrapper.send(member, request).retry(2)).verifyComplete();
    }

    @Test
    void sendShouldWorkMemberThreeAddresses() {
      final List<Address> addresses =
          Arrays.asList(Address.from("test:0"), Address.from("test:1"), Address.from("test:2"));
      final Member member = new Member("test", null, addresses, "namespace");
      final Transport transport = mock(Transport.class);
      final TransportWrapper transportWrapper = new TransportWrapper(transport);

      when(transport.send(addresses.get(0), request))
          .thenReturn(Mono.error(new RuntimeException("Error")));
      when(transport.send(addresses.get(1), request))
          .thenReturn(Mono.error(new RuntimeException("Error")));
      when(transport.send(addresses.get(2), request)).thenReturn(Mono.empty());

      StepVerifier.create(transportWrapper.send(member, request).retry(2)).verifyComplete();
    }

    @Test
    void sendShouldFailMemberSingleAddress() {
      final List<Address> addresses = Collections.singletonList(Address.from("test:0"));
      final Member member = new Member("test", null, addresses, "namespace");
      final Transport transport = mock(Transport.class);
      final TransportWrapper transportWrapper = new TransportWrapper(transport);

      when(transport.send(addresses.get(0), request))
          .thenReturn(Mono.error(new RuntimeException("Error")));

      StepVerifier.create(transportWrapper.send(member, request).retry(2))
          .verifyErrorSatisfies(
              throwable -> Assertions.assertEquals("Error", throwable.getMessage()));
    }

    @Test
    void sendShouldFailMemberTwoAddresses() {
      final List<Address> addresses = Arrays.asList(Address.from("test:0"), Address.from("test:1"));
      final Member member = new Member("test", null, addresses, "namespace");
      final Transport transport = mock(Transport.class);
      final TransportWrapper transportWrapper = new TransportWrapper(transport);

      when(transport.send(addresses.get(0), request))
          .thenReturn(Mono.error(new RuntimeException("Error - 0")));
      when(transport.send(addresses.get(1), request))
          .thenReturn(Mono.error(new RuntimeException("Error - 1")));

      StepVerifier.create(transportWrapper.send(member, request).retry(2))
          .verifyErrorSatisfies(
              throwable -> Assertions.assertEquals("Error - 1", throwable.getMessage()));
    }

    @Test
    void sendShouldFailMemberThreeAddresses() {
      final List<Address> addresses =
          Arrays.asList(Address.from("test:0"), Address.from("test:1"), Address.from("test:2"));
      final Member member = new Member("test", null, addresses, "namespace");
      final Transport transport = mock(Transport.class);
      final TransportWrapper transportWrapper = new TransportWrapper(transport);

      when(transport.send(addresses.get(0), request))
          .thenReturn(Mono.error(new RuntimeException("Error - 0")));
      when(transport.send(addresses.get(1), request))
          .thenReturn(Mono.error(new RuntimeException("Error - 1")));
      when(transport.send(addresses.get(2), request))
          .thenReturn(Mono.error(new RuntimeException("Error - 2")));

      StepVerifier.create(transportWrapper.send(member, request).retry(2))
          .verifyErrorSatisfies(
              throwable -> Assertions.assertEquals("Error - 2", throwable.getMessage()));
    }
  }
}
