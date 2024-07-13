package io.scalecube.cluster2.membership;

import static io.scalecube.cluster2.sbe.MemberActionType.ADD_MEMBER;
import static io.scalecube.cluster2.sbe.MemberStatus.ALIVE;

import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberActionCodec;
import io.scalecube.cluster2.sbe.MemberStatus;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;

public class MembershipTable {

  private final MembershipRecord localRecord;
  private final BroadcastTransmitter messageTx;

  private final Member localMember;
  private final MemberActionCodec memberActionCodec = new MemberActionCodec();
  private final MembershipRecordCodec membershipRecordCodec = new MembershipRecordCodec();
  private final Map<UUID, MembershipRecord> recordMap = new Object2ObjectHashMap<>();

  public MembershipTable(MembershipRecord localRecord, BroadcastTransmitter messageTx) {
    this.localRecord = localRecord;
    this.messageTx = messageTx;
    localMember = localRecord.member();
    recordMap.put(localMember.id(), localRecord);
  }

  public void put(MembershipRecord record) {
    final Member member = record.member();
    final UUID key = member.id();
    final MembershipRecord oldRecord = recordMap.get(key);
    if (oldRecord == null) {
      recordMap.put(key, record);
      emitAddMember(member);
      return;
    }

    if (record.incarnation() < oldRecord.incarnation()) {
      return;
    }

    if (localMember.equals(member) && record.status() != ALIVE) {
      localRecord.incarnation(localRecord.incarnation() + 1);
      emitGossip(localRecord);
      return;
    }

    if (record.incarnation() > oldRecord.incarnation()) {
      recordMap.put(key, record);
      return;
    }

    if (record.status() != ALIVE) {
      recordMap.put(key, record);
    }
  }

  public void put(Member member, MemberStatus status) {
    final MembershipRecord record = recordMap.get(member.id());
    if (record != null && record.status() != status) {
      record.status(status);
      emitGossip(record);
    }
  }

  public void forEach(Consumer<MembershipRecord> consumer) {
    recordMap.values().forEach(consumer);
  }

  private void emitAddMember(final Member member) {
    messageTx.transmit(
        1, memberActionCodec.encode(ADD_MEMBER, member), 0, memberActionCodec.encodedLength());
  }

  private void emitGossip(MembershipRecord record) {
    messageTx.transmit(
        1, membershipRecordCodec.encode(record), 0, membershipRecordCodec.encodedLength());
  }
}
