package io.scalecube.cluster2.fdetector;

import static org.agrona.concurrent.broadcast.BroadcastBufferDescriptor.TRAILER_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster.transport.api2.Transport.MessagePoller;
import io.scalecube.cluster2.CallbackInvoker;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.membership.MembershipEventCodec;
import io.scalecube.cluster2.sbe.FailureDetectorEventDecoder;
import io.scalecube.cluster2.sbe.MemberStatus;
import io.scalecube.cluster2.sbe.MembershipEventType;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
import io.scalecube.cluster2.sbe.PingAckDecoder;
import io.scalecube.cluster2.sbe.PingDecoder;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class FailureDetectorTest {

  private final Member localMember = new Member(UUID.randomUUID(), "address:1180");
  private final Member fooMember = new Member(UUID.randomUUID(), "address:1181");
  private final Member barMember = new Member(UUID.randomUUID(), "address:1182");
  private final Member aliceMember = new Member(UUID.randomUUID(), "address:1183");

  private Transport transport;
  private FailureDetector failureDetector;
  private CallbackInvoker callbackInvoker;
  private final ExpandableDirectByteBuffer byteBuffer =
      new ExpandableDirectByteBuffer(1024 * 1024 + TRAILER_LENGTH);
  private final BroadcastTransmitter messageTx =
      new BroadcastTransmitter(new UnsafeBuffer(byteBuffer));
  private final Supplier<CopyBroadcastReceiver> messageRxSupplier =
      () -> new CopyBroadcastReceiver(new BroadcastReceiver(new UnsafeBuffer(byteBuffer)));
  private final CachedEpochClock epochClock = new CachedEpochClock();
  private final FailureDetectorConfig config = new FailureDetectorConfig();
  private final MessagePoller messagePoller = mock(MessagePoller.class);
  private final ArgumentCaptor<String> addressCaptor = ArgumentCaptor.forClass(String.class);

  @BeforeEach
  void beforeEach() {
    transport = mock(Transport.class);
    callbackInvoker = mock(CallbackInvoker.class);

    when(transport.newMessagePoller()).thenReturn(messagePoller);

    failureDetector =
        new FailureDetector(
            transport,
            messageTx,
            messageRxSupplier,
            epochClock,
            callbackInvoker,
            config,
            localMember);
  }

  @Test
  void testOnTickWithNoPingMembers() {
    advanceClock(1);

    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnMembershipEventLocalMemberWillBeFiltered() {
    emitMembershipEvent(MembershipEventType.ADDED, localMember);

    advanceClock(1);

    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnMembershipEventAddedThenRemoved() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.REMOVED, fooMember);

    advanceClock(1);

    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnMembershipEventAddedThenLeaving() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.LEAVING, fooMember);

    advanceClock(1);

    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnTick() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);

    advanceClock(1);
    verify(transport).send(addressCaptor.capture(), any(), anyInt(), anyInt());
    assertEquals(fooMember.address(), addressCaptor.getValue(), "fooMember.address");

    reset(transport);
    advanceClock(config.pingInterval() + 1);
    verify(transport).send(addressCaptor.capture(), any(), anyInt(), anyInt());
    assertEquals(fooMember.address(), addressCaptor.getValue(), "fooMember.address");

    reset(transport);
    advanceClock(config.pingInterval() + 1);
    verify(transport).send(addressCaptor.capture(), any(), anyInt(), anyInt());
    assertEquals(fooMember.address(), addressCaptor.getValue(), "fooMember.address");
  }

  @Test
  void testPingThenAck() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);

    advanceClock(1);
    verify(transport).send(addressCaptor.capture(), any(), anyInt(), anyInt());
    assertEquals(fooMember.address(), addressCaptor.getValue(), "fooMember.address");

    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
    emitMessageFromTransport(
        codec -> codec.encodePingAck(failureDetector.currentCid(), localMember, fooMember, null));

    assertMessageRx(
        messageRx,
        (memberStatus, member) -> {
          assertEquals(MemberStatus.ALIVE, memberStatus, "memberStatus");
          assertEquals(fooMember, member, "member");
        });
  }

  @Test
  void testPingThenTimeout() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);

    advanceClock(1);
    verify(transport).send(addressCaptor.capture(), any(), anyInt(), anyInt());
    assertEquals(fooMember.address(), addressCaptor.getValue(), "fooMember.address");

    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
    advanceClock(config.pingTimeout() + 1);

    assertMessageRx(
        messageRx,
        (memberStatus, member) -> {
          assertEquals(MemberStatus.SUSPECT, memberStatus, "memberStatus");
          assertEquals(fooMember, member, "member");
        });
  }

  @Test
  void testPingThenTimeoutThenPingRequestThenAck() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, barMember);

    advanceClock(1);
    verify(transport).send(addressCaptor.capture(), any(), anyInt(), anyInt());
    assertThat(addressCaptor.getValue(), anyOf(is(fooMember.address()), is(barMember.address())));

    reset(transport);
    advanceClock(config.pingTimeout() + 1);

    verify(transport).send(addressCaptor.capture(), any(), anyInt(), anyInt());
    assertThat(addressCaptor.getValue(), anyOf(is(fooMember.address()), is(barMember.address())));

    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
    emitMessageFromTransport(
        codec -> codec.encodePingAck(failureDetector.currentCid(), localMember, fooMember, null));

    assertMessageRx(
        messageRx,
        (memberStatus, member) -> {
          assertEquals(MemberStatus.ALIVE, memberStatus, "memberStatus");
          assertEquals(fooMember, member, "member");
        });
  }

  @Test
  void testPingThenTimeoutThenPingRequestThenTimeout() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, barMember);

    advanceClock(1);
    verify(transport).send(addressCaptor.capture(), any(), anyInt(), anyInt());
    assertThat(addressCaptor.getValue(), anyOf(is(fooMember.address()), is(barMember.address())));

    reset(transport);
    advanceClock(config.pingTimeout() + 1);

    verify(transport).send(addressCaptor.capture(), any(), anyInt(), anyInt());
    assertThat(addressCaptor.getValue(), anyOf(is(fooMember.address()), is(barMember.address())));

    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
    advanceClock(config.pingTimeout() + 1);

    assertMessageRx(
        messageRx,
        (memberStatus, member) -> {
          assertEquals(MemberStatus.SUSPECT, memberStatus, "memberStatus");
          // assertEquals(fooMember, member, "member");
        });
  }

  @Nested
  class PingTests {

    @Test
    void testOnPing() {}

    @Test
    void testOnPingWithIssuer() {}

    @Test
    void testOnPingWithNonMatchingTarget() {}
  }

  @Nested
  class PingRequestTests {

    @Test
    void testOnPingRequest() {
      final long cid = 100;

      emitMessageFromTransport(codec -> codec.encodePingRequest(cid, barMember, fooMember));

      verify(transport)
          .send(
              eq(fooMember.address()),
              argThat(
                  arg -> {
                    final MutableDirectBuffer buffer = (MutableDirectBuffer) arg;
                    final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
                    final PingDecoder decoder = new PingDecoder();
                    final MemberCodec memberCodec = new MemberCodec();

                    headerDecoder.wrap(buffer, 0);
                    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);

                    assertEquals(PingDecoder.TEMPLATE_ID, headerDecoder.templateId());
                    assertEquals(cid, decoder.cid());

                    final Member from = memberCodec.member(decoder::wrapFrom);
                    final Member target = memberCodec.member(decoder::wrapTarget);
                    final Member issuer = memberCodec.member(decoder::wrapIssuer);

                    assertEquals(localMember, from);
                    assertEquals(fooMember, target);
                    assertEquals(barMember, issuer);

                    return true;
                  }),
              anyInt(),
              anyInt());
    }
  }

  @Nested
  class PingAckTests {

    @Test
    void testPingAck() {
      final long cid = 100;

      emitMessageFromTransport(codec -> codec.encodePingAck(cid, localMember, fooMember, null));

      verify(callbackInvoker)
          .invokeCallback(
              eq(cid),
              argThat(
                  arg -> {
                    assertEquals(fooMember, arg);
                    return true;
                  }));

      verify(transport, never()).send(any(), any(), anyInt(), anyInt());
    }

    @Test
    void testPingAckWithIssuer() {
      final long cid = 100;

      emitMessageFromTransport(
          codec -> codec.encodePingAck(cid, barMember, fooMember, localMember));

      verify(callbackInvoker, never()).invokeCallback(anyInt(), any());

      verify(transport)
          .send(
              eq(localMember.address()),
              argThat(
                  arg -> {
                    final MutableDirectBuffer buffer = (MutableDirectBuffer) arg;
                    final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
                    final PingAckDecoder decoder = new PingAckDecoder();
                    final MemberCodec memberCodec = new MemberCodec();

                    headerDecoder.wrap(buffer, 0);
                    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);

                    assertEquals(PingAckDecoder.TEMPLATE_ID, headerDecoder.templateId());
                    assertEquals(cid, decoder.cid());

                    final Member from = memberCodec.member(decoder::wrapFrom);
                    final Member target = memberCodec.member(decoder::wrapTarget);
                    final Member issuer = memberCodec.member(decoder::wrapIssuer);

                    assertEquals(localMember, from);
                    assertEquals(fooMember, target);
                    assertNull(issuer);

                    return true;
                  }),
              anyInt(),
              anyInt());
    }

    @Test
    void testPingAckWithNonMatchingFrom() {
      final long cid = 100;

      emitMessageFromTransport(codec -> codec.encodePingAck(cid, aliceMember, fooMember, null));

      verify(callbackInvoker, never()).invokeCallback(anyInt(), any());
      verify(transport, never()).send(any(), any(), anyInt(), anyInt());
    }

    @Test
    void testPingAckWithNonMatchingPeriod() {
      // TODO
    }
  }

  private void advanceClock(final long millis) {
    epochClock.advance(millis);
    failureDetector.doWork();
  }

  private void emitMembershipEvent(MembershipEventType eventType, Member member) {
    final MembershipEventCodec membershipEventCodec = new MembershipEventCodec();
    messageTx.transmit(
        1,
        membershipEventCodec.encodeMembershipEvent(eventType, 1, member),
        0,
        membershipEventCodec.encodedLength());
    failureDetector.doWork();
  }

  private void emitMessageFromTransport(
      Function<FailureDetectorCodec, MutableDirectBuffer> function) {
    final FailureDetectorCodec failureDetectorCodec = new FailureDetectorCodec();
    doAnswer(
            invocation -> {
              final MessageHandler messageHandler = (MessageHandler) invocation.getArguments()[0];
              messageHandler.onMessage(
                  1, function.apply(failureDetectorCodec), 0, failureDetectorCodec.encodedLength());
              return 1;
            })
        .when(messagePoller)
        .poll(any());
    failureDetector.doWork();
  }

  private void assertMessageRx(
      CopyBroadcastReceiver messageRx, BiConsumer<MemberStatus, Member> consumer) {
    final MutableReference<FailureDetectorEventDecoder> mutableReference = new MutableReference<>();
    final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    final FailureDetectorEventDecoder failureDetectorEventDecoder =
        new FailureDetectorEventDecoder();
    final MemberCodec memberCodec = new MemberCodec();

    messageRx.receive(
        (msgTypeId, buffer, index, length) -> {
          // no-op first time
        });
    messageRx.receive(
        (msgTypeId, buffer, index, length) ->
            mutableReference.set(
                failureDetectorEventDecoder.wrapAndApplyHeader(buffer, index, headerDecoder)));

    final FailureDetectorEventDecoder decoder = mutableReference.get();
    if (decoder == null) {
      consumer.accept(null, null);
      return;
    }

    consumer.accept(decoder.status(), memberCodec.member(decoder::wrapMember));
  }
}
