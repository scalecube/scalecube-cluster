package io.scalecube.cluster2.membership;

import static io.scalecube.cluster2.sbe.MemberStatus.ALIVE;
import static org.agrona.concurrent.broadcast.BroadcastBufferDescriptor.TRAILER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.scalecube.cluster2.Member;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.junit.jupiter.api.Test;

class MembershipTableTest {

  private final MembershipRecord localRecord =
      new MembershipRecord()
          .incarnation(0)
          .status(ALIVE)
          .alias("alias")
          .namespace("ns")
          .member(new Member(UUID.randomUUID(), "address:1180"));

  private final EpochClock epochClock = new CachedEpochClock();
  private final ExpandableDirectByteBuffer byteBuffer =
      new ExpandableDirectByteBuffer(1024 * 1024 + TRAILER_LENGTH);
  private final BroadcastTransmitter messageTx =
      new BroadcastTransmitter(new UnsafeBuffer(byteBuffer));
  private final MembershipTable membershipTable =
      new MembershipTable(epochClock, messageTx, localRecord);

  @Test
  void testDoNothing() {
    final Map<UUID, MembershipRecord> recordMap = recordMap();
    assertEquals(1, recordMap.size());
    assertEquals(localRecord, recordMap.get(localRecord.member().id()));
    assertEquals(0, membershipTable.doWork());
  }

  @Test
  void testMemberAdded() {
    fail("Implement");
  }

  @Test
  void testMemberRemoved() {
    fail("Implement");
  }

  @Test
  void testMemberNotUpdatedWhenIncarnationLessThanExisting() {
    fail("Implement");
  }

  @Test
  void testLocalMemberUpdated() {
    fail("Implement");
  }

  @Test
  void testMemberUpdatedByGreaterIncarnation() {
    fail("Implement");
  }

  @Test
  void testMemberUpdatedByAssumingTheWorse() {
    fail("Implement");
  }

  @Test
  void testMemberNotUpdatedIfNotAssumingTheWorse() {
    fail("Implement");
  }

  @Test
  void testMemberNotUpdatedIfThereAreNoChanges() {
    fail("Implement");
  }

  @Test
  void testAliveMemberChangedToSuspected() {
    fail("Implement");
  }

  @Test
  void testSuspectedMemberChangedToAlive() {
    fail("Implement");
  }

  @Test
  void testApplySuspectedStatusOnSuspectedMember() {
    fail("Implement");
  }

  @Test
  void testApplySuspectedStatusOnAliveMember() {
    fail("Implement");
  }

  @Test
  void testApplyAliveStatusOnAliveMember() {
    fail("Implement");
  }

  @Test
  void testApplyAliveStatusOnSuspectedMember() {
    fail("Implement");
  }

  private Map<UUID, MembershipRecord> recordMap() {
    final HashMap<UUID, MembershipRecord> recordMap = new HashMap<>();
    membershipTable.forEach(record -> recordMap.put(record.member().id(), record));
    return recordMap;
  }
}
