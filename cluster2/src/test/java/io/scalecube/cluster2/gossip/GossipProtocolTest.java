package io.scalecube.cluster2.gossip;

import static io.scalecube.cluster2.sbe.MemberActionType.ADD_MEMBER;
import static io.scalecube.cluster2.sbe.MemberActionType.REMOVE_MEMBER;
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
import io.scalecube.cluster2.ClusterMath;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberActionCodec;
import io.scalecube.cluster2.sbe.GossipMessageEncoder;
import io.scalecube.cluster2.sbe.MemberActionType;
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

  private final Member localMember = new Member(UUID.randomUUID(), "address:1180");
  private final Member fooMember = new Member(UUID.randomUUID(), "address:1181");
  private final Member barMember = new Member(UUID.randomUUID(), "address:1182");
  private final Member aliceMember = new Member(UUID.randomUUID(), "address:1183");

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
    advanceClock(1);
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnMemberActionLocalMemberWillBeFiltered() {
    emitMemberAction(ADD_MEMBER, localMember);
    advanceClock(1);
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnMemberActionAddThenRemove() {
    emitMemberAction(ADD_MEMBER, fooMember);
    emitMemberAction(ADD_MEMBER, fooMember);
    emitMemberAction(ADD_MEMBER, fooMember);
    emitMemberAction(REMOVE_MEMBER, fooMember);

    advanceClock(1);

    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnTickNoGossips() {
    emitMemberAction(ADD_MEMBER, fooMember);
    emitMemberAction(ADD_MEMBER, barMember);
    emitMemberAction(ADD_MEMBER, aliceMember);

    advanceClock(1);

    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testOnGossipMessage() {
    emitMemberAction(ADD_MEMBER, fooMember);
    emitMemberAction(ADD_MEMBER, barMember);
    emitMemberAction(ADD_MEMBER, aliceMember);

    emitGossipMessage(newMessage());

    advanceClock(1);

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
    emitMemberAction(ADD_MEMBER, fooMember);
    emitMemberAction(ADD_MEMBER, barMember);
    emitMemberAction(ADD_MEMBER, aliceMember);

    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
    final byte[] message = newMessage();

    emitGossipRequest(
        codec -> codec.encode(UUID.randomUUID(), new Gossip(UUID.randomUUID(), 1, message, 1)));

    assertMessageRx(
        messageRx, messageFromRx -> assertArrayEquals(message, messageFromRx, "gossip.message"));

    reset(messagePoller);
    advanceClock(1);
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
    emitMemberAction(ADD_MEMBER, fooMember);
    emitMemberAction(ADD_MEMBER, barMember);
    emitMemberAction(ADD_MEMBER, aliceMember);

    final byte[] message = newMessage();
    emitGossipMessage(message);

    emitGossipRequest(
        codec -> codec.encode(fooMember.id(), new Gossip(localMember.id(), 1, message, 1)));
    emitGossipRequest(
        codec -> codec.encode(barMember.id(), new Gossip(localMember.id(), 1, message, 1)));

    advanceClock(1);

    verify(transport).send(addressCaptor.capture(), any(), anyInt(), anyInt());
    assertThat(addressCaptor.getValue(), is(aliceMember.address()));
  }

  @Test
  void testShouldNotSpreadGossipToInfected() {
    emitMemberAction(ADD_MEMBER, fooMember);
    emitMemberAction(ADD_MEMBER, barMember);
    emitMemberAction(ADD_MEMBER, aliceMember);

    final byte[] message = newMessage();
    emitGossipMessage(message);

    emitGossipRequest(
        codec -> codec.encode(fooMember.id(), new Gossip(localMember.id(), 1, message, 1)));
    emitGossipRequest(
        codec -> codec.encode(barMember.id(), new Gossip(localMember.id(), 1, message, 1)));
    emitGossipRequest(
        codec -> codec.encode(aliceMember.id(), new Gossip(localMember.id(), 1, message, 1)));

    advanceClock(1);

    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  void testShouldNotSpreadGossipWhenNoRemoteMembers() {
    emitGossipMessage(newMessage());

    emitGossipRequest(
        codec ->
            codec.encode(UUID.randomUUID(), new Gossip(UUID.randomUUID(), 1, newMessage(), 1)));

    advanceClock(1);

    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  @Test
  public void testSweepGossips() {
    emitMemberAction(ADD_MEMBER, fooMember);
    emitMemberAction(ADD_MEMBER, barMember);
    emitMemberAction(ADD_MEMBER, aliceMember);

    emitGossipMessage(newMessage());

    advanceClock(1);

    verify(transport, times(3)).send(any(), any(), anyInt(), anyInt());

    final int periodsToSweep = ClusterMath.gossipPeriodsToSweep(config.gossipRepeatMult(), 3 + 1);
    for (int i = 0; i < periodsToSweep; i++) {
      reset(transport);
      advanceClock(config.gossipInterval() + 1);
    }

    reset(transport);
    advanceClock(config.gossipInterval() + 1);
    verify(transport, never()).send(any(), any(), anyInt(), anyInt());
  }

  private static byte[] newMessage() {
    final Random random = new Random();
    final byte[] bytes = new byte[128];
    random.nextBytes(bytes);
    return bytes;
  }

  private void advanceClock(final long millis) {
    epochClock.advance(millis);
    gossipProtocol.doWork();
  }

  private void emitMemberAction(MemberActionType actionType, Member member) {
    final MemberActionCodec memberActionCodec = new MemberActionCodec();
    messageTx.transmit(
        1, memberActionCodec.encode(actionType, member), 0, memberActionCodec.encodedLength());
    gossipProtocol.doWork();
  }

  private void emitGossipMessage(byte[] message) {
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    final GossipMessageEncoder gossipMessageEncoder = new GossipMessageEncoder();
    gossipMessageEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    gossipMessageEncoder.putMessage(message, 0, message.length);
    final int encodedLength = headerEncoder.encodedLength() + gossipMessageEncoder.encodedLength();
    messageTx.transmit(1, buffer, 0, encodedLength);
    gossipProtocol.doWork();
  }

  private void emitGossipRequest(Function<GossipRequestCodec, MutableDirectBuffer> function) {
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
    gossipProtocol.doWork();
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
