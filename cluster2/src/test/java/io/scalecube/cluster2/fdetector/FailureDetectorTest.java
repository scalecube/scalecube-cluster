package io.scalecube.cluster2.fdetector;

import static org.agrona.concurrent.broadcast.BroadcastBufferDescriptor.TRAILER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster.transport.api2.Transport.MessagePoller;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.membership.MembershipEventCodec;
import io.scalecube.cluster2.sbe.FailureDetectorEventDecoder;
import io.scalecube.cluster2.sbe.MemberStatus;
import io.scalecube.cluster2.sbe.MembershipEventType;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
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
import org.junit.jupiter.api.Test;

class FailureDetectorTest {

  private final Member localMember = new Member(UUID.randomUUID(), "local", "address:1180", "ns");
  private final Member fooMember = new Member(UUID.randomUUID(), "foo", "address:1181", "ns");
  private final Member barMember = new Member(UUID.randomUUID(), "bar", "address:1182", "ns");
  private final Member aliceMember = new Member(UUID.randomUUID(), "alice", "address:1183", "ns");
  private final Member bobMember = new Member(UUID.randomUUID(), "bob", "address:1184", "ns");

  private final Transport transport = mock(Transport.class);
  private final ExpandableDirectByteBuffer byteBuffer =
      new ExpandableDirectByteBuffer(1024 * 1024 + TRAILER_LENGTH);
  private final BroadcastTransmitter messageTx =
      new BroadcastTransmitter(new UnsafeBuffer(byteBuffer));
  private final Supplier<CopyBroadcastReceiver> messageRxSupplier =
      () -> new CopyBroadcastReceiver(new BroadcastReceiver(new UnsafeBuffer(byteBuffer)));
  private final CachedEpochClock epochClock = new CachedEpochClock();
  private FailureDetectorConfig config = new FailureDetectorConfig();
  private FailureDetector failureDetector =
      new FailureDetector(transport, messageTx, messageRxSupplier, epochClock, config, localMember);
  private final MembershipEventCodec membershipEventCodec = new MembershipEventCodec();
  private final FailureDetectorCodec failureDetectorCodec = new FailureDetectorCodec();
  private final MessagePoller messagePoller = mock(MessagePoller.class);

  @BeforeEach
  void beforeEach() {
    when(transport.newMessagePoller()).thenReturn(messagePoller);
  }

  @Test
  void testTickWhenNoPingMembers() {
    epochClock.advance(1);
    failureDetector.doWork();
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnMembershipEventLocalMemberWillBeFiltered() {
    emitMembershipEvent(MembershipEventType.ADDED, localMember);
    failureDetector.doWork();

    epochClock.advance(1);
    failureDetector.doWork();
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnMembershipEventAddedThenRemoved() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    failureDetector.doWork();
    failureDetector.doWork();
    failureDetector.doWork();

    emitMembershipEvent(MembershipEventType.REMOVED, fooMember);
    failureDetector.doWork();

    epochClock.advance(1);
    failureDetector.doWork();
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnMembershipEventAddedThenLeaving() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    failureDetector.doWork();
    failureDetector.doWork();
    failureDetector.doWork();

    emitMembershipEvent(MembershipEventType.LEAVING, fooMember);
    failureDetector.doWork();

    epochClock.advance(1);
    failureDetector.doWork();
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnTick() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    failureDetector.doWork();

    epochClock.advance(1);
    failureDetector.doWork();
    verify(transport).send(any(), any(), anyInt(), anyInt());

    reset(transport);
    epochClock.advance(config.pingInterval() + 1);
    failureDetector.doWork();
    verify(transport).send(any(), any(), anyInt(), anyInt());

    reset(transport);
    epochClock.advance(config.pingInterval() + 1);
    failureDetector.doWork();
    verify(transport).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testPingThenAck() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    failureDetector.doWork();

    epochClock.advance(1);
    failureDetector.doWork();
    verify(transport).send(any(), any(), anyInt(), anyInt());

    emitMessageFromTransport(
        codec -> codec.encodePingAck(failureDetector.currentCid(), localMember, fooMember, null));
    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
    failureDetector.doWork();

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
    failureDetector.doWork();

    epochClock.advance(1);
    failureDetector.doWork();
    verify(transport).send(any(), any(), anyInt(), anyInt());

    epochClock.advance(config.pingTimeout() + 1);
    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
    failureDetector.doWork();

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
    failureDetector.doWork();
    failureDetector.doWork();

    epochClock.advance(1);
    failureDetector.doWork();
    verify(transport).send(any(), any(), anyInt(), anyInt());

    reset(transport);
    epochClock.advance(config.pingTimeout() + 1);
    failureDetector.doWork();

    verify(transport).send(any(), any(), anyInt(), anyInt());

    emitMessageFromTransport(
        codec -> codec.encodePingAck(failureDetector.currentCid(), localMember, fooMember, null));
    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
    failureDetector.doWork();

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
    failureDetector.doWork();
    failureDetector.doWork();

    epochClock.advance(1);
    failureDetector.doWork();
    verify(transport).send(any(), any(), anyInt(), anyInt());

    reset(transport);
    epochClock.advance(config.pingTimeout() + 1);
    failureDetector.doWork();

    verify(transport).send(any(), any(), anyInt(), anyInt());

    epochClock.advance(config.pingTimeout() + 1);
    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
    failureDetector.doWork();

    assertMessageRx(
        messageRx,
        (memberStatus, member) -> {
          assertEquals(MemberStatus.SUSPECT, memberStatus, "memberStatus");
          // assertEquals(fooMember, member, "member");
        });
  }

  private void emitMembershipEvent(MembershipEventType eventType, Member member) {
    messageTx.transmit(
        1,
        membershipEventCodec.encodeMembershipEvent(eventType, 1, member),
        0,
        membershipEventCodec.encodedLength());
  }

  private void emitMessageFromTransport(
      Function<FailureDetectorCodec, MutableDirectBuffer> function) {
    doAnswer(
            invocation -> {
              final MessageHandler messageHandler = (MessageHandler) invocation.getArguments()[0];
              messageHandler.onMessage(
                  1, function.apply(failureDetectorCodec), 0, failureDetectorCodec.encodedLength());
              return 1;
            })
        .when(messagePoller)
        .poll(any());
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
