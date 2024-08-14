//package io.scalecube.cluster2.membership;
//
//import static io.scalecube.cluster2.sbe.MemberStatus.ALIVE;
//import static io.scalecube.cluster2.sbe.MemberStatus.SUSPECTED;
//import static org.agrona.concurrent.broadcast.BroadcastBufferDescriptor.TRAILER_LENGTH;
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertNotNull;
//import static org.junit.jupiter.api.Assertions.assertNull;
//
//import io.scalecube.cluster2.ClusterMath;
//import io.scalecube.cluster2.Member;
//import io.scalecube.cluster2.MemberCodec;
//import io.scalecube.cluster2.sbe.AddMemberDecoder;
//import io.scalecube.cluster2.sbe.GossipOutputMessageDecoder;
//import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
//import io.scalecube.cluster2.sbe.PayloadGenerationEventDecoder;
//import io.scalecube.cluster2.sbe.RemoveMemberDecoder;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Random;
//import java.util.UUID;
//import java.util.function.Consumer;
//import java.util.function.Supplier;
//import org.agrona.ExpandableDirectByteBuffer;
//import org.agrona.collections.MutableReference;
//import org.agrona.concurrent.CachedEpochClock;
//import org.agrona.concurrent.UnsafeBuffer;
//import org.agrona.concurrent.broadcast.BroadcastReceiver;
//import org.agrona.concurrent.broadcast.BroadcastTransmitter;
//import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
//import org.junit.jupiter.api.Test;
//
//class MembershipTableTest {
//
//  private static final int SUSPICION_MULT = 3;
//  private static final int PING_INTERVAL = 1000;
//  private static final String NAMESPACE = "ns";
//  private static final int INITIAL_INCARNATION = 0;
//
//  private final MembershipRecord localRecord =
//      new MembershipRecord()
//          .incarnation(INITIAL_INCARNATION)
//          .status(ALIVE)
//          .alias("alias@" + System.currentTimeMillis())
//          .namespace(NAMESPACE)
//          .member(new Member(UUID.randomUUID(), "address:1180"));
//
//  private final CachedEpochClock epochClock = new CachedEpochClock();
//  private final ExpandableDirectByteBuffer byteBuffer =
//      new ExpandableDirectByteBuffer(1024 * 1024 + TRAILER_LENGTH);
//  private final BroadcastTransmitter messageTx =
//      new BroadcastTransmitter(new UnsafeBuffer(byteBuffer));
//  private final Supplier<CopyBroadcastReceiver> messageRxSupplier =
//      () -> new CopyBroadcastReceiver(new BroadcastReceiver(new UnsafeBuffer(byteBuffer)));
//  private final ArrayList<Member> remoteMembers = new ArrayList<>();
//  private final MembershipTable membershipTable =
//      new MembershipTable(
//          epochClock, messageTx, localRecord, remoteMembers, SUSPICION_MULT, PING_INTERVAL);
//
//  @Test
//  void testDoNothing() {
//    final Map<UUID, MembershipRecord> recordMap = recordMap();
//
//    assertEquals(1, recordMap.size());
//    final MembershipRecord record = recordMap.get(localRecord.member().id());
//    assertEquals(localRecord, record);
//    assertEquals(ALIVE, record.status());
//    assertEquals(INITIAL_INCARNATION, record.incarnation());
//
//    assertEquals(0, membershipTable.doWork());
//  }
//
//  @Test
//  void testMemberAdded() {
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//    final MembershipRecord record = newRecord();
//
//    membershipTable.put(record);
//
//    assertAddMember(messageRx, member -> assertEquals(record.member(), member, "member"), false);
//
//    assertEquals(1, remoteMembers.size());
//    assertEquals(record.member(), remoteMembers.get(0));
//  }
//
//  @Test
//  void testMemberRemoved() {
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//    final MembershipRecord record = newRecord();
//    final MembershipRecord suspectedRecord = copyFrom(record, r -> r.status(SUSPECTED));
//
//    membershipTable.put(record);
//    membershipTable.put(suspectedRecord);
//
//    assertEquals(1, remoteMembers.size());
//    assertEquals(record.member(), remoteMembers.get(0));
//
//    advanceClock(suspicionTimeout() + 1);
//
//    assertPayloadGenerationEvent(
//        messageRx, decoder -> assertNotNull(decoder, "assertPayloadGenerationEvent"), true);
//    assertRemoveMember(messageRx, member -> assertEquals(record.member(), member, "member"), false);
//
//    assertEquals(0, remoteMembers.size());
//  }
//
//  @Test
//  void testMemberRemovedByApplyingMemberStatus() {
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//    final MembershipRecord record = newRecord();
//
//    membershipTable.put(record);
//    membershipTable.put(record.member(), SUSPECTED);
//
//    assertPayloadGenerationEvent(
//        messageRx, decoder -> assertNotNull(decoder, "assertPayloadGenerationEvent"), true);
//    assertGossipMessage(messageRx, mr -> assertEquals(SUSPECTED, mr.status()), false);
//
//    assertEquals(1, remoteMembers.size());
//    assertEquals(record.member(), remoteMembers.get(0));
//
//    advanceClock(suspicionTimeout() + 1);
//
//    assertRemoveMember(messageRx, member -> assertEquals(record.member(), member, "member"), false);
//
//    assertEquals(0, remoteMembers.size());
//  }
//
//  @Test
//  void testMemberRecoveredByApplyingMemberStatus() {
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//    final MembershipRecord record = newRecord();
//
//    membershipTable.put(record);
//    membershipTable.put(record.member(), SUSPECTED);
//
//    assertPayloadGenerationEvent(
//        messageRx, decoder -> assertNotNull(decoder, "assertPayloadGenerationEvent"), true);
//    assertGossipMessage(messageRx, mr -> assertEquals(SUSPECTED, mr.status()), false);
//
//    assertEquals(1, remoteMembers.size());
//    assertEquals(record.member(), remoteMembers.get(0));
//
//    membershipTable.put(record.member(), ALIVE);
//
//    assertGossipMessage(messageRx, mr -> assertEquals(ALIVE, mr.status()), false);
//
//    advanceClock(suspicionTimeout() + 1);
//
//    assertEquals(1, remoteMembers.size());
//  }
//
//  @Test
//  void testMemberNotUpdatedWhenIncarnationLessThanExisting() {
//    final int incarnation = 2;
//    final MembershipRecord record = newRecord(r -> r.incarnation(incarnation));
//    final MembershipRecord suspectedRecord =
//        copyFrom(record, r -> r.incarnation(1).status(SUSPECTED));
//
//    membershipTable.put(record);
//    membershipTable.put(suspectedRecord);
//
//    assertEquals(localRecord, recordMap().get(localRecord.member().id()));
//    assertEquals(record, recordMap().get(record.member().id()));
//    assertEquals(ALIVE, record.status());
//    assertEquals(incarnation, record.incarnation());
//  }
//
//  @Test
//  void testLocalMemberUpdated() {
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//
//    membershipTable.put(copyFrom(localRecord, r -> r.status(SUSPECTED)));
//
//    final MembershipRecord record = recordMap().get(localRecord.member().id());
//    assertEquals(1, record.incarnation());
//    assertEquals(SUSPECTED, record.status());
//
//    assertGossipMessage(
//        messageRx,
//        mr -> {
//          assertEquals(INITIAL_INCARNATION + 1, mr.incarnation(), "incarnation");
//          assertEquals(SUSPECTED, mr.status());
//        },
//        false);
//  }
//
//  @Test
//  void testLocalMemberNotUpdated() {
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//
//    membershipTable.put(copyFrom(localRecord));
//
//    final MembershipRecord record = recordMap().get(localRecord.member().id());
//    assertEquals(INITIAL_INCARNATION, record.incarnation());
//    assertEquals(ALIVE, record.status());
//
//    assertGossipMessage(messageRx, mr -> assertNull(mr, "assertGossipMessage"), false);
//  }
//
//  @Test
//  void testAliveMemberUpdatedByGreaterIncarnation() {
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//    final MembershipRecord record = newRecord();
//    final MembershipRecord suspectedRecord =
//        copyFrom(record, r -> r.status(SUSPECTED).incarnation(1));
//
//    membershipTable.put(record);
//    membershipTable.put(suspectedRecord);
//
//    assertPayloadGenerationEvent(
//        messageRx, decoder -> assertNotNull(decoder, "assertPayloadGenerationEvent"), true);
//    assertGossipMessage(messageRx, mr -> assertNull(mr, "assertGossipMessage"), false);
//
//    final MembershipRecord recordAfterUpdate = recordMap().get(record.member().id());
//    assertEquals(SUSPECTED, recordAfterUpdate.status());
//    assertEquals(INITIAL_INCARNATION + 1, recordAfterUpdate.incarnation());
//  }
//
//  @Test
//  void testSuspectedMemberUpdatedByGreaterIncarnation() {
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//    final MembershipRecord record = newRecord(r -> r.status(SUSPECTED));
//    final MembershipRecord suspectedRecord = copyFrom(record, r -> r.status(ALIVE).incarnation(1));
//
//    membershipTable.put(record);
//    membershipTable.put(suspectedRecord);
//
//    assertPayloadGenerationEvent(
//        messageRx, decoder -> assertNotNull(decoder, "assertPayloadGenerationEvent"), true);
//    assertGossipMessage(messageRx, mr -> assertNull(mr, "assertGossipMessage"), false);
//
//    final MembershipRecord recordAfterUpdate = recordMap().get(record.member().id());
//    assertEquals(ALIVE, recordAfterUpdate.status());
//    assertEquals(INITIAL_INCARNATION + 1, recordAfterUpdate.incarnation());
//  }
//
//  @Test
//  void testMemberUpdatedByAssumingTheWorse() {
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//    final MembershipRecord record = newRecord();
//    final MembershipRecord suspectedRecord = copyFrom(record, r -> r.status(SUSPECTED));
//
//    membershipTable.put(record);
//    membershipTable.put(suspectedRecord);
//
//    assertPayloadGenerationEvent(
//        messageRx, decoder -> assertNotNull(decoder, "assertPayloadGenerationEvent"), true);
//    assertGossipMessage(messageRx, mr -> assertNull(mr, "assertGossipMessage"), false);
//
//    final MembershipRecord recordAfterUpdate = recordMap().get(record.member().id());
//    assertEquals(SUSPECTED, recordAfterUpdate.status());
//    assertEquals(INITIAL_INCARNATION, recordAfterUpdate.incarnation());
//  }
//
//  @Test
//  void testMemberNotUpdatedOptimistically() {
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//    final MembershipRecord record = newRecord(r -> r.status(SUSPECTED));
//    final MembershipRecord suspectedRecord = copyFrom(record, r -> r.status(ALIVE));
//
//    membershipTable.put(record);
//    membershipTable.put(suspectedRecord);
//
//    assertPayloadGenerationEvent(
//        messageRx, decoder -> assertNotNull(decoder, "assertPayloadGenerationEvent"), true);
//    assertGossipMessage(messageRx, mr -> assertNull(mr, "assertGossipMessage"), false);
//
//    final MembershipRecord recordAfterUpdate = recordMap().get(record.member().id());
//    assertEquals(SUSPECTED, recordAfterUpdate.status());
//    assertEquals(INITIAL_INCARNATION, recordAfterUpdate.incarnation());
//  }
//
//  @Test
//  void testApplySuspectedStatusOnSuspectedMember() {
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//    final MembershipRecord record = newRecord(r -> r.status(SUSPECTED));
//
//    membershipTable.put(record);
//    membershipTable.put(record.member(), SUSPECTED);
//
//    assertPayloadGenerationEvent(
//        messageRx, decoder -> assertNotNull(decoder, "assertPayloadGenerationEvent"), true);
//    assertGossipMessage(messageRx, mr -> assertNull(mr, "assertGossipMessage"), false);
//
//    assertEquals(SUSPECTED, recordMap().get(record.member().id()).status());
//  }
//
//  @Test
//  void testApplySuspectedStatusOnAliveMember() {
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//    final MembershipRecord record = newRecord();
//
//    membershipTable.put(record);
//    membershipTable.put(record.member(), SUSPECTED);
//
//    assertPayloadGenerationEvent(
//        messageRx, decoder -> assertNotNull(decoder, "assertPayloadGenerationEvent"), true);
//    assertGossipMessage(messageRx, mr -> assertEquals(SUSPECTED, mr.status()), false);
//
//    assertEquals(SUSPECTED, recordMap().get(record.member().id()).status());
//  }
//
//  @Test
//  void testApplyAliveStatusOnAliveMember() {
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//    final MembershipRecord record = newRecord();
//
//    membershipTable.put(record);
//    membershipTable.put(record.member(), ALIVE);
//
//    assertPayloadGenerationEvent(
//        messageRx, decoder -> assertNotNull(decoder, "assertPayloadGenerationEvent"), true);
//    assertGossipMessage(messageRx, mr -> assertNull(mr, "assertGossipMessage"), false);
//
//    assertEquals(ALIVE, recordMap().get(record.member().id()).status());
//  }
//
//  @Test
//  void testApplyAliveStatusOnSuspectedMember() {
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//    final MembershipRecord record = newRecord(r -> r.status(SUSPECTED));
//
//    membershipTable.put(record);
//    membershipTable.put(record.member(), ALIVE);
//
//    assertPayloadGenerationEvent(
//        messageRx, decoder -> assertNotNull(decoder, "assertPayloadGenerationEvent"), true);
//    assertGossipMessage(messageRx, mr -> assertEquals(ALIVE, mr.status()), false);
//
//    assertEquals(ALIVE, recordMap().get(record.member().id()).status());
//  }
//
//  @Test
//  void testFilterByNamespace() {
//    membershipTable.put(newRecord(r -> r.namespace(UUID.randomUUID().toString())));
//
//    final Map<UUID, MembershipRecord> recordMap = recordMap();
//
//    assertEquals(1, recordMap.size());
//    assertEquals(localRecord, recordMap.get(localRecord.member().id()));
//  }
//
//  @Test
//  void testPayloadGenerationEvent() {
//    final int payloadLength = 100;
//
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//    final MembershipRecord record = newRecord();
//    final MembershipRecord recordWithGreaterGeneration =
//        copyFrom(record, r -> r.payloadLength(payloadLength));
//
//    membershipTable.put(record);
//    membershipTable.put(recordWithGreaterGeneration);
//
//    assertPayloadGenerationEvent(
//        messageRx,
//        decoder -> {
//          assertNotNull(decoder, "assertPayloadGenerationEvent");
//          assertEquals(0, decoder.payloadLength(), "payloadLength");
//        },
//        true);
//  }
//
//  @Test
//  void testPayloadGenerationNotUpdatedForLocalMember() {
//    final int payloadLength = 100;
//
//    final CopyBroadcastReceiver messageRx = messageRxSupplier.get();
//    final MembershipRecord localRecordWithGreaterGeneration =
//        copyFrom(localRecord, r -> r.payloadLength(payloadLength));
//
//    membershipTable.put(localRecordWithGreaterGeneration);
//
//    assertPayloadGenerationEvent(
//        messageRx, decoder -> assertNull(decoder, "assertPayloadGenerationEvent"), true);
//  }
//
//  private void advanceClock(final long millis) {
//    epochClock.advance(millis);
//    membershipTable.doWork();
//  }
//
//  private Map<UUID, MembershipRecord> recordMap() {
//    final HashMap<UUID, MembershipRecord> recordMap = new HashMap<>();
//    membershipTable.forEach(record -> recordMap.put(record.member().id(), record));
//    return recordMap;
//  }
//
//  private void assertAddMember(
//      CopyBroadcastReceiver messageRx, Consumer<Member> consumer, boolean skipLast) {
//    final MutableReference<AddMemberDecoder> mutableReference = new MutableReference<>();
//    final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
//    final AddMemberDecoder addMemberDecoder = new AddMemberDecoder();
//    final MemberCodec memberCodec = new MemberCodec();
//
//    if (skipLast) {
//      messageRx.receive(
//          (msgTypeId, buffer, index, length) -> {
//            // skip last
//          });
//    }
//
//    messageRx.receive(
//        (msgTypeId, buffer, index, length) -> {
//          final AddMemberDecoder decoder =
//              addMemberDecoder.wrapAndApplyHeader(buffer, index, headerDecoder);
//          mutableReference.set(decoder);
//        });
//
//    final AddMemberDecoder decoder = mutableReference.get();
//    if (decoder == null) {
//      consumer.accept(null);
//      return;
//    }
//
//    consumer.accept(memberCodec.member(decoder::wrapMember));
//  }
//
//  private void assertRemoveMember(
//      CopyBroadcastReceiver messageRx, Consumer<Member> consumer, boolean skipLast) {
//    final MutableReference<RemoveMemberDecoder> mutableReference = new MutableReference<>();
//    final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
//    final RemoveMemberDecoder removeMemberDecoder = new RemoveMemberDecoder();
//    final MemberCodec memberCodec = new MemberCodec();
//
//    if (skipLast) {
//      messageRx.receive(
//          (msgTypeId, buffer, index, length) -> {
//            // skip last
//          });
//    }
//
//    messageRx.receive(
//        (msgTypeId, buffer, index, length) -> {
//          final RemoveMemberDecoder decoder =
//              removeMemberDecoder.wrapAndApplyHeader(buffer, index, headerDecoder);
//          mutableReference.set(decoder);
//        });
//
//    final RemoveMemberDecoder decoder = mutableReference.get();
//    if (decoder == null) {
//      consumer.accept(null);
//      return;
//    }
//
//    consumer.accept(memberCodec.member(decoder::wrapMember));
//  }
//
//  private void assertGossipMessage(
//      CopyBroadcastReceiver messageRx, Consumer<MembershipRecord> consumer, boolean skipLast) {
//    final MutableReference<GossipOutputMessageDecoder> mutableReference = new MutableReference<>();
//    final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
//    final GossipOutputMessageDecoder gossipOutputMessageDecoder = new GossipOutputMessageDecoder();
//
//    if (skipLast) {
//      messageRx.receive(
//          (msgTypeId, buffer, index, length) -> {
//            // skip last
//          });
//    }
//
//    messageRx.receive(
//        (msgTypeId, buffer, index, length) -> {
//          final GossipOutputMessageDecoder decoder =
//              gossipOutputMessageDecoder.wrapAndApplyHeader(buffer, index, headerDecoder);
//          mutableReference.set(decoder);
//        });
//
//    final GossipOutputMessageDecoder decoder = mutableReference.get();
//    if (decoder == null) {
//      consumer.accept(null);
//      return;
//    }
//
//    consumer.accept(new MembershipRecordCodec().membershipRecord(decoder::wrapMessage));
//  }
//
//  private void assertPayloadGenerationEvent(
//      CopyBroadcastReceiver messageRx,
//      Consumer<PayloadGenerationEventDecoder> consumer,
//      boolean skipLast) {
//    final MutableReference<PayloadGenerationEventDecoder> mutableReference =
//        new MutableReference<>();
//    final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
//    final PayloadGenerationEventDecoder payloadGenerationEventDecoder =
//        new PayloadGenerationEventDecoder();
//
//    if (skipLast) {
//      messageRx.receive(
//          (msgTypeId, buffer, index, length) -> {
//            // skip last
//          });
//    }
//
//    messageRx.receive(
//        (msgTypeId, buffer, index, length) -> {
//          final PayloadGenerationEventDecoder decoder =
//              payloadGenerationEventDecoder.wrapAndApplyHeader(buffer, index, headerDecoder);
//          mutableReference.set(decoder);
//        });
//
//    consumer.accept(mutableReference.get());
//  }
//
//  private static MembershipRecord newRecord() {
//    return newRecord(null);
//  }
//
//  private static MembershipRecord newRecord(Consumer<MembershipRecord> consumer) {
//    final Random random = new Random();
//    final int port = random.nextInt(65536);
//    final Member member = new Member().id(UUID.randomUUID()).address("foobar:" + port);
//    final MembershipRecord membershipRecord =
//        new MembershipRecord()
//            .incarnation(INITIAL_INCARNATION)
//            .status(ALIVE)
//            .alias("alias@" + System.currentTimeMillis())
//            .namespace(NAMESPACE)
//            .member(member);
//    if (consumer != null) {
//      consumer.accept(membershipRecord);
//    }
//    return membershipRecord;
//  }
//
//  private static MembershipRecord copyFrom(MembershipRecord record) {
//    return copyFrom(record, null);
//  }
//
//  private static MembershipRecord copyFrom(
//      MembershipRecord record, Consumer<MembershipRecord> consumer) {
//    final MembershipRecord membershipRecord =
//        new MembershipRecord()
//            .incarnation(record.incarnation())
//            .status(record.status())
//            .alias(record.alias())
//            .namespace(record.namespace())
//            .member(record.member());
//    if (consumer != null) {
//      consumer.accept(membershipRecord);
//    }
//    return membershipRecord;
//  }
//
//  private long suspicionTimeout() {
//    return ClusterMath.suspicionTimeout(SUSPICION_MULT, membershipTable.size(), PING_INTERVAL);
//  }
//}
