package io.scalecube.cluster2.membership;

import static io.scalecube.cluster2.sbe.MemberStatus.ALIVE;
import static org.agrona.concurrent.broadcast.BroadcastBufferDescriptor.TRAILER_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster.transport.api2.Transport.MessagePoller;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.fdetector.FailureDetectorCodec;
import io.scalecube.cluster2.fdetector.FailureDetectorConfig;
import io.scalecube.cluster2.gossip.GossipMessageCodec;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MembershipProtocolTest {

  private static final String NAMESPACE = "ns";

  private final String fooAddress = "foo:1181";
  private final String barAddress = "bar:1182";
  private final String bazAddress = "baz:1183";

  private final MembershipRecord localRecord =
      new MembershipRecord()
          .incarnation(0)
          .status(ALIVE)
          .alias("alias@" + System.currentTimeMillis())
          .namespace(NAMESPACE)
          .member(new Member(UUID.randomUUID(), "address:1180"));

  private final Transport transport = mock(Transport.class);
  private final ExpandableDirectByteBuffer byteBuffer =
      new ExpandableDirectByteBuffer(1024 * 1024 + TRAILER_LENGTH);
  private final BroadcastTransmitter messageTx =
      new BroadcastTransmitter(new UnsafeBuffer(byteBuffer));
  private final Supplier<CopyBroadcastReceiver> messageRxSupplier =
      () -> new CopyBroadcastReceiver(new BroadcastReceiver(new UnsafeBuffer(byteBuffer)));
  private final CachedEpochClock epochClock = new CachedEpochClock();
  private final MessagePoller messagePoller = mock(MessagePoller.class);
  private final FailureDetectorConfig fdetectorConfig = new FailureDetectorConfig();
  private MembershipConfig config;
  private MembershipProtocol membershipProtocol;

  @BeforeEach
  void beforeEach() {
    when(transport.newMessagePoller()).thenReturn(messagePoller);
  }

  @Test
  void testDoNothing() {
    config = new MembershipConfig();
    membershipProtocol = newMembershipProtocol();

    assertEquals(0, membershipProtocol.doWork());
  }

  @Test
  void testTick() {
    config = new MembershipConfig().seedMembers(fooAddress, barAddress, bazAddress);
    membershipProtocol = newMembershipProtocol();

    advanceClock(config.syncInterval() + 1);

    verify(transport, times(2))
        .send(
            argThat(
                arg -> {
                  assertThat(arg, isOneOf(fooAddress, barAddress, bazAddress));
                  return true;
                }),
            argThat(arg -> true),
            anyInt(),
            anyInt());
  }

  private void advanceClock(final long millis) {
    epochClock.advance(millis);
    membershipProtocol.doWork();
  }

  private MembershipProtocol newMembershipProtocol() {
    return new MembershipProtocol(
        transport, messageTx, messageRxSupplier, epochClock, config, fdetectorConfig, localRecord);
  }

  private void emitSync(Function<SyncCodec, MutableDirectBuffer> function) {
    final SyncCodec codec = new SyncCodec();
    doAnswer(
            invocation -> {
              final MessageHandler messageHandler = (MessageHandler) invocation.getArguments()[0];
              messageHandler.onMessage(1, function.apply(codec), 0, codec.encodedLength());
              return 1;
            })
        .when(messagePoller)
        .poll(any());
    membershipProtocol.doWork();
  }

  private void emitSyncAck(Function<SyncCodec, MutableDirectBuffer> function) {
    final SyncCodec codec = new SyncCodec();
    doAnswer(
            invocation -> {
              final MessageHandler messageHandler = (MessageHandler) invocation.getArguments()[0];
              messageHandler.onMessage(1, function.apply(codec), 0, codec.encodedLength());
              return 1;
            })
        .when(messagePoller)
        .poll(any());
    membershipProtocol.doWork();
  }

  private void emitGossipInputMessage(Function<GossipMessageCodec, MutableDirectBuffer> function) {
    final GossipMessageCodec codec = new GossipMessageCodec();
    doAnswer(
            invocation -> {
              final MessageHandler messageHandler = (MessageHandler) invocation.getArguments()[0];
              messageHandler.onMessage(1, function.apply(codec), 0, codec.encodedLength());
              return 1;
            })
        .when(messagePoller)
        .poll(any());
    membershipProtocol.doWork();
  }

  private void emitFailureDetectorEvent(
      Function<FailureDetectorCodec, MutableDirectBuffer> function) {
    final FailureDetectorCodec codec = new FailureDetectorCodec();
    messageTx.transmit(1, function.apply(codec), 0, codec.encodedLength());
    membershipProtocol.doWork();
  }
}
