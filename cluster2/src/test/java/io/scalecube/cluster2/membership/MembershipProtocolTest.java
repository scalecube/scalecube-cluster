//package io.scalecube.cluster2.membership;
//
//import static io.scalecube.cluster2.sbe.MemberStatus.ALIVE;
//import static org.agrona.concurrent.broadcast.BroadcastBufferDescriptor.TRAILER_LENGTH;
//import static org.hamcrest.MatcherAssert.assertThat;
//import static org.hamcrest.Matchers.isOneOf;
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.ArgumentMatchers.anyInt;
//import static org.mockito.ArgumentMatchers.argThat;
//import static org.mockito.ArgumentMatchers.eq;
//import static org.mockito.Mockito.doAnswer;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.never;
//import static org.mockito.Mockito.times;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;
//
//import io.scalecube.cluster.transport.api2.Transport;
//import io.scalecube.cluster.transport.api2.Transport.MessagePoller;
//import io.scalecube.cluster2.ClusterConfig;
//import io.scalecube.cluster2.Member;
//import io.scalecube.cluster2.fdetector.FailureDetectorCodec;
//import io.scalecube.cluster2.gossip.GossipMessageCodec;
//import io.scalecube.cluster2.sbe.MemberStatus;
//import java.lang.reflect.Field;
//import java.util.UUID;
//import java.util.function.Consumer;
//import java.util.function.Function;
//import java.util.function.Supplier;
//import org.agrona.ExpandableDirectByteBuffer;
//import org.agrona.LangUtil;
//import org.agrona.MutableDirectBuffer;
//import org.agrona.collections.MutableLong;
//import org.agrona.concurrent.CachedEpochClock;
//import org.agrona.concurrent.MessageHandler;
//import org.agrona.concurrent.UnsafeBuffer;
//import org.agrona.concurrent.broadcast.BroadcastReceiver;
//import org.agrona.concurrent.broadcast.BroadcastTransmitter;
//import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.mockito.Mockito;
//
//class MembershipProtocolTest {
//
//  private static final String NAMESPACE = "ns";
//
//  private final String localAddress = "local:1181";
//  private final String fooAddress = "foo:1181";
//  private final String barAddress = "bar:1182";
//  private final String bazAddress = "baz:1183";
//
//  private final MembershipRecord localRecord =
//      new MembershipRecord()
//          .incarnation(0)
//          .status(ALIVE)
//          .alias("alias@" + System.currentTimeMillis())
//          .namespace(NAMESPACE)
//          .member(new Member(UUID.randomUUID(), localAddress));
//
//  private final Transport transport = mock(Transport.class);
//  private final ExpandableDirectByteBuffer byteBuffer =
//      new ExpandableDirectByteBuffer(1024 * 1024 + TRAILER_LENGTH);
//  private final BroadcastTransmitter messageTx =
//      new BroadcastTransmitter(new UnsafeBuffer(byteBuffer));
//  private final Supplier<CopyBroadcastReceiver> messageRxSupplier =
//      () -> new CopyBroadcastReceiver(new BroadcastReceiver(new UnsafeBuffer(byteBuffer)));
//  private final CachedEpochClock epochClock = new CachedEpochClock();
//  private final MessagePoller messagePoller = mock(MessagePoller.class);
//  private final ClusterConfig config = new ClusterConfig();
//  private MembershipProtocol membershipProtocol;
//  private MembershipTable membershipTable;
//
//  @BeforeEach
//  void beforeEach() {
//    when(transport.newMessagePoller()).thenReturn(messagePoller);
//    ensureMembershipProtocol();
//  }
//
//  @Test
//  void testDoNothing() {
//    assertEquals(0, membershipProtocol.doWork());
//  }
//
//  @Test
//  void testTick() {
//    ensureMembershipProtocol(config -> config.seedMembers(fooAddress, barAddress, bazAddress));
//
//    advanceClock(config.syncInterval() + 1);
//
//    verify(transport, times(2))
//        .send(
//            argThat(
//                arg -> {
//                  assertThat(arg, isOneOf(fooAddress, barAddress, bazAddress));
//                  return true;
//                }),
//            argThat(arg -> true),
//            anyInt(),
//            anyInt());
//  }
//
//  @Test
//  void testOnSync() {
//    emitSync(syncCodec -> syncCodec.encodeSync(Long.MIN_VALUE, null, localRecord));
//
//    verify(membershipTable)
//        .put(
//            argThat(
//                arg -> {
//                  assertMembershipRecordEquals(localRecord, arg);
//                  return true;
//                }));
//  }
//
//  @Test
//  void testOnSyncComplete() {
//    emitSync(syncCodec -> syncCodec.encodeSync(10, fooAddress, null));
//
//    verify(membershipTable, never()).put(any(MembershipRecord.class));
//
//    verify(transport)
//        .send(
//            argThat(
//                arg -> {
//                  assertEquals(fooAddress, arg, "from");
//                  return true;
//                }),
//            argThat(arg -> true),
//            anyInt(),
//            anyInt());
//  }
//
//  @Test
//  void testOnSyncAck() {
//    final int period = 10;
//    setMembershipProtocolPeriod(period);
//
//    emitSyncAck(syncCodec -> syncCodec.encodeSyncAck(period, localRecord));
//
//    verify(membershipTable)
//        .put(
//            argThat(
//                arg -> {
//                  assertMembershipRecordEquals(localRecord, arg);
//                  return true;
//                }));
//  }
//
//  @Test
//  void testOnSyncAckPeriodMismatch() {
//    final int period = 10;
//    setMembershipProtocolPeriod(period);
//    emitSyncAck(syncCodec -> syncCodec.encodeSyncAck(20, localRecord));
//    verify(membershipTable, never()).put(any(MembershipRecord.class));
//  }
//
//  @Test
//  void testOnGossipMessage() {
//    emitGossipInputMessage(
//        codec -> {
//          final MembershipRecordCodec membershipRecordCodec = new MembershipRecordCodec();
//          final MutableDirectBuffer buffer = membershipRecordCodec.encode(localRecord);
//          return codec.encodeInputMessage(buffer, 0, membershipRecordCodec.encodedLength());
//        });
//
//    verify(membershipTable)
//        .put(
//            argThat(
//                arg -> {
//                  assertMembershipRecordEquals(localRecord, arg);
//                  return true;
//                }));
//  }
//
//  @Test
//  void testOnFailureDetectorEvent() {
//    final Member member = localRecord.member();
//    final MemberStatus memberStatus = ALIVE;
//    emitFailureDetectorEvent(codec -> codec.encodeFailureDetectorEvent(member, memberStatus));
//
//    verify(membershipTable)
//        .put(
//            argThat(
//                arg -> {
//                  assertEquals(member, arg, "member");
//                  return true;
//                }),
//            eq(memberStatus));
//  }
//
//  private void advanceClock(final long millis) {
//    epochClock.advance(millis);
//    membershipProtocol.doWork();
//  }
//
//  private void ensureMembershipProtocol() {
//    ensureMembershipProtocol(null);
//  }
//
//  private void ensureMembershipProtocol(Consumer<ClusterConfig> consumer) {
//    try {
//      if (consumer != null) {
//        consumer.accept(config);
//      }
//      membershipProtocol =
//          new MembershipProtocol(
//              transport, messageTx, messageRxSupplier, epochClock, config, localRecord);
//      final Class<? extends MembershipProtocol> clazz = membershipProtocol.getClass();
//      final Field field = clazz.getDeclaredField("membershipTable");
//      field.setAccessible(true);
//      membershipTable = Mockito.spy((MembershipTable) field.get(membershipProtocol));
//      field.set(membershipProtocol, membershipTable);
//    } catch (Exception ex) {
//      LangUtil.rethrowUnchecked(ex);
//    }
//  }
//
//  private void setMembershipProtocolPeriod(long val) {
//    try {
//      final Class<? extends MembershipProtocol> clazz = membershipProtocol.getClass();
//      final Field field = clazz.getDeclaredField("period");
//      field.setAccessible(true);
//      final MutableLong period = (MutableLong) field.get(membershipProtocol);
//      period.set(val);
//    } catch (Exception ex) {
//      LangUtil.rethrowUnchecked(ex);
//    }
//  }
//
//  private void emitSync(Function<SyncCodec, MutableDirectBuffer> function) {
//    final SyncCodec syncCodec = new SyncCodec();
//    doAnswer(
//            invocation -> {
//              final MessageHandler messageHandler = (MessageHandler) invocation.getArguments()[0];
//              messageHandler.onMessage(1, function.apply(syncCodec), 0, syncCodec.encodedLength());
//              return 1;
//            })
//        .when(messagePoller)
//        .poll(any());
//    membershipProtocol.doWork();
//  }
//
//  private void emitSyncAck(Function<SyncCodec, MutableDirectBuffer> function) {
//    final SyncCodec syncCodec = new SyncCodec();
//    doAnswer(
//            invocation -> {
//              final MessageHandler messageHandler = (MessageHandler) invocation.getArguments()[0];
//              messageHandler.onMessage(1, function.apply(syncCodec), 0, syncCodec.encodedLength());
//              return 1;
//            })
//        .when(messagePoller)
//        .poll(any());
//    membershipProtocol.doWork();
//  }
//
//  private void emitGossipInputMessage(Function<GossipMessageCodec, MutableDirectBuffer> function) {
//    final GossipMessageCodec gossipMessageCodec = new GossipMessageCodec();
//    doAnswer(
//            invocation -> {
//              final MessageHandler messageHandler = (MessageHandler) invocation.getArguments()[0];
//              messageHandler.onMessage(
//                  1, function.apply(gossipMessageCodec), 0, gossipMessageCodec.encodedLength());
//              return 1;
//            })
//        .when(messagePoller)
//        .poll(any());
//    membershipProtocol.doWork();
//  }
//
//  private void emitFailureDetectorEvent(
//      Function<FailureDetectorCodec, MutableDirectBuffer> function) {
//    final FailureDetectorCodec failureDetectorCodec = new FailureDetectorCodec();
//    messageTx.transmit(
//        1, function.apply(failureDetectorCodec), 0, failureDetectorCodec.encodedLength());
//    membershipProtocol.doWork();
//  }
//
//  private static void assertMembershipRecordEquals(
//      MembershipRecord expected, MembershipRecord actual) {
//    assertEquals(expected.incarnation(), actual.incarnation(), "incarnation");
//    assertEquals(expected.status(), actual.status(), "status");
//    assertEquals(expected.alias(), actual.alias(), "alias");
//    assertEquals(expected.namespace(), actual.namespace(), "namespace");
//    assertEquals(expected.member(), actual.member(), "member");
//  }
//}