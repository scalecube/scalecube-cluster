package io.scalecube.cluster2.gossip;

import static org.agrona.concurrent.broadcast.BroadcastBufferDescriptor.TRAILER_LENGTH;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster.transport.api2.Transport.MessagePoller;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.membership.MembershipEventCodec;
import io.scalecube.cluster2.sbe.GossipMessageEncoder;
import io.scalecube.cluster2.sbe.MembershipEventType;
import io.scalecube.cluster2.sbe.MessageHeaderEncoder;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.agrona.ExpandableArrayBuffer;
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
import org.mockito.ArgumentCaptor;

class GossipProtocolTest {

  private final Member localMember = new Member(UUID.randomUUID(), "local", "address:1180", "ns");
  private final Member fooMember = new Member(UUID.randomUUID(), "foo", "address:1181", "ns");
  private final Member barMember = new Member(UUID.randomUUID(), "bar", "address:1182", "ns");
  private final Member aliceMember = new Member(UUID.randomUUID(), "alice", "address:1183", "ns");

  private final Transport transport = mock(Transport.class);
  private final ExpandableDirectByteBuffer byteBuffer =
      new ExpandableDirectByteBuffer(1024 * 1024 + TRAILER_LENGTH);
  private final BroadcastTransmitter messageTx =
      new BroadcastTransmitter(new UnsafeBuffer(byteBuffer));
  private final Supplier<CopyBroadcastReceiver> messageRxSupplier =
      () -> new CopyBroadcastReceiver(new BroadcastReceiver(new UnsafeBuffer(byteBuffer)));
  private final CachedEpochClock epochClock = new CachedEpochClock();
  private final GossipConfig config = new GossipConfig();
  private final GossipProtocol gossipProtocol =
      new GossipProtocol(transport, messageTx, messageRxSupplier, epochClock, config, localMember);
  private final MessagePoller messagePoller = mock(MessagePoller.class);
  private final ArgumentCaptor<String> addressCaptor = ArgumentCaptor.forClass(String.class);

  @BeforeEach
  void beforeEach() {
    when(transport.newMessagePoller()).thenReturn(messagePoller);
  }

  @Test
  void testTickWhenNoRemoteMembers() {
    epochClock.advance(1);
    gossipProtocol.doWork();
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnMembershipEventLocalMemberWillBeFiltered() {
    emitMembershipEvent(MembershipEventType.ADDED, localMember);
    gossipProtocol.doWork();

    epochClock.advance(1);
    gossipProtocol.doWork();
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnMembershipEventAddedThenRemoved() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    gossipProtocol.doWork();
    gossipProtocol.doWork();
    gossipProtocol.doWork();

    emitMembershipEvent(MembershipEventType.REMOVED, fooMember);
    gossipProtocol.doWork();

    epochClock.advance(1);
    gossipProtocol.doWork();
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnMembershipEventAddedThenLeaving() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    gossipProtocol.doWork();
    gossipProtocol.doWork();
    gossipProtocol.doWork();

    emitMembershipEvent(MembershipEventType.LEAVING, fooMember);
    gossipProtocol.doWork();

    epochClock.advance(1);
    gossipProtocol.doWork();
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnTickNoGossips() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, barMember);
    emitMembershipEvent(MembershipEventType.ADDED, aliceMember);
    gossipProtocol.doWork();
    gossipProtocol.doWork();
    gossipProtocol.doWork();

    epochClock.advance(1);
    gossipProtocol.doWork();
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnGossipMessage() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, barMember);
    emitMembershipEvent(MembershipEventType.ADDED, aliceMember);
    gossipProtocol.doWork();
    gossipProtocol.doWork();
    gossipProtocol.doWork();

    emitGossipMessage(newMessage());
    gossipProtocol.doWork();

    epochClock.advance(1);
    gossipProtocol.doWork();

    verify(transport, times(3)).send(addressCaptor.capture(), any(), anyInt(), anyInt());
    assertThat(
        addressCaptor.getAllValues(),
        allOf(
            hasItem(fooMember.address()),
            hasItem(barMember.address()),
            hasItem(aliceMember.address())));
  }

  @Test
  void testOnGossipRequest() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, barMember);
    emitMembershipEvent(MembershipEventType.ADDED, aliceMember);
    gossipProtocol.doWork();
    gossipProtocol.doWork();
    gossipProtocol.doWork();

    final byte[] message = newMessage();
    emitMessageFromTransport(
        codec -> codec.encode(UUID.randomUUID(), new Gossip(UUID.randomUUID(), 1, message, 1)));
    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
    gossipProtocol.doWork();

    assertMessageRx(
        messageRx, messageFromRx -> assertArrayEquals(message, messageFromRx, "gossip.message"));

    reset(messagePoller);
    epochClock.advance(1);
    gossipProtocol.doWork();

    verify(transport, times(3)).send(addressCaptor.capture(), any(), anyInt(), anyInt());
    assertThat(
        addressCaptor.getAllValues(),
        allOf(
            hasItem(fooMember.address()),
            hasItem(barMember.address()),
            hasItem(aliceMember.address())));
  }

  @Test
  void testSpreadGossipOnlyToNonInfected() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, barMember);
    emitMembershipEvent(MembershipEventType.ADDED, aliceMember);
    gossipProtocol.doWork();
    gossipProtocol.doWork();
    gossipProtocol.doWork();

    final byte[] message = newMessage();
    emitGossipMessage(message);
    gossipProtocol.doWork();

    emitMessageFromTransport(
        codec -> codec.encode(fooMember.id(), new Gossip(localMember.id(), 1, message, 1)));
    gossipProtocol.doWork();
    emitMessageFromTransport(
        codec -> codec.encode(barMember.id(), new Gossip(localMember.id(), 1, message, 1)));
    gossipProtocol.doWork();

    epochClock.advance(1);
    gossipProtocol.doWork();

    verify(transport).send(addressCaptor.capture(), any(), anyInt(), anyInt());
    assertThat(addressCaptor.getValue(), is(aliceMember.address()));
  }

  @Test
  void testShouldNotSpreadGossipToInfected() {
    emitMembershipEvent(MembershipEventType.ADDED, fooMember);
    emitMembershipEvent(MembershipEventType.ADDED, barMember);
    emitMembershipEvent(MembershipEventType.ADDED, aliceMember);
    gossipProtocol.doWork();
    gossipProtocol.doWork();
    gossipProtocol.doWork();

    final byte[] message = newMessage();
    emitGossipMessage(message);
    gossipProtocol.doWork();

    emitMessageFromTransport(
        codec -> codec.encode(fooMember.id(), new Gossip(localMember.id(), 1, message, 1)));
    gossipProtocol.doWork();
    emitMessageFromTransport(
        codec -> codec.encode(barMember.id(), new Gossip(localMember.id(), 1, message, 1)));
    gossipProtocol.doWork();
    emitMessageFromTransport(
        codec -> codec.encode(aliceMember.id(), new Gossip(localMember.id(), 1, message, 1)));
    gossipProtocol.doWork();

    epochClock.advance(1);
    gossipProtocol.doWork();

    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testShouldNotSpreadGossipWhenNoRemoteMembers() {
    emitGossipMessage(newMessage());
    emitMessageFromTransport(
        codec ->
            codec.encode(UUID.randomUUID(), new Gossip(UUID.randomUUID(), 1, newMessage(), 1)));
    gossipProtocol.doWork();

    epochClock.advance(1);
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  private static byte[] newMessage() {
    final Random random = new Random();
    final byte[] bytes = new byte[128];
    random.nextBytes(bytes);
    return bytes;
  }

  private void emitMembershipEvent(MembershipEventType eventType, Member member) {
    final MembershipEventCodec membershipEventCodec = new MembershipEventCodec();
    messageTx.transmit(
        1,
        membershipEventCodec.encodeMembershipEvent(eventType, 1, member),
        0,
        membershipEventCodec.encodedLength());
  }

  private void emitGossipMessage(byte[] message) {
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    final GossipMessageEncoder gossipMessageEncoder = new GossipMessageEncoder();
    gossipMessageEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    gossipMessageEncoder.putMessage(message, 0, message.length);
    final int encodedLength = headerEncoder.encodedLength() + gossipMessageEncoder.encodedLength();
    messageTx.transmit(1, buffer, 0, encodedLength);
  }

  private void emitMessageFromTransport(
      Function<GossipRequestCodec, MutableDirectBuffer> function) {
    final GossipRequestCodec gossipRequestCodec = new GossipRequestCodec();
    doAnswer(
            invocation -> {
              final MessageHandler messageHandler = (MessageHandler) invocation.getArguments()[0];
              messageHandler.onMessage(
                  1, function.apply(gossipRequestCodec), 0, gossipRequestCodec.encodedLength());
              return 1;
            })
        .when(messagePoller)
        .poll(any());
  }

  private void assertMessageRx(CopyBroadcastReceiver messageRx, Consumer<byte[]> consumer) {
    final MutableReference<byte[]> mutableReference = new MutableReference<>();
    messageRx.receive(
        (msgTypeId, buffer, index, length) -> {
          // no-op first time
        });
    messageRx.receive(
        (msgTypeId, buffer, index, length) -> {
          final byte[] message = new byte[length];
          buffer.getBytes(index, message, 0, message.length);
          mutableReference.set(message);
        });

    consumer.accept(mutableReference.get());
  }
}
