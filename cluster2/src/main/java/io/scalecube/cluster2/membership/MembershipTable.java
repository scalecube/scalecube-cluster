package io.scalecube.cluster2.membership;

import static io.scalecube.cluster2.sbe.MemberActionType.ADD_MEMBER;
import static io.scalecube.cluster2.sbe.MemberActionType.REMOVE_MEMBER;
import static io.scalecube.cluster2.sbe.MemberStatus.ALIVE;

import io.scalecube.cluster2.ClusterMath;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberActionCodec;
import io.scalecube.cluster2.TimerInvoker;
import io.scalecube.cluster2.gossip.GossipMessageCodec;
import io.scalecube.cluster2.sbe.MemberActionType;
import io.scalecube.cluster2.sbe.MemberStatus;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2LongHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;

public class MembershipTable {

  private final EpochClock epochClock;
  private final BroadcastTransmitter messageTx;
  private final MembershipRecord localRecord;
  private final ArrayList<Member> remoteMembers;
  private final Member localMember;
  private final int suspicionMult;
  private final int pingInterval;

  private final Object2LongHashMap<UUID> timerIdByMemberId =
      new Object2LongHashMap<>(Long.MIN_VALUE);
  private final Long2ObjectHashMap<UUID> memberIdByTimerId = new Long2ObjectHashMap<>();
  private final TimerInvoker timerInvoker;
  private final MemberActionCodec memberActionCodec = new MemberActionCodec();
  private final MembershipRecordCodec membershipRecordCodec = new MembershipRecordCodec();
  private final GossipMessageCodec gossipMessageCodec = new GossipMessageCodec();
  private final Map<UUID, MembershipRecord> recordMap = new Object2ObjectHashMap<>();

  public MembershipTable(
      EpochClock epochClock,
      BroadcastTransmitter messageTx,
      MembershipRecord localRecord,
      ArrayList<Member> remoteMembers,
      int suspicionMult,
      int pingInterval) {
    this.epochClock = epochClock;
    this.messageTx = messageTx;
    this.localRecord = localRecord;
    this.remoteMembers = remoteMembers;
    this.suspicionMult = suspicionMult;
    this.pingInterval = pingInterval;
    timerInvoker = new TimerInvoker(epochClock);
    localMember = localRecord.member();
    recordMap.put(localMember.id(), localRecord);
  }

  public int doWork() {
    return timerInvoker.poll(this::onTimerExpiry);
  }

  public void put(MembershipRecord record) {
    final String namespace = record.namespace();
    if (!localRecord.namespace().equals(namespace)) {
      return;
    }

    final Member member = record.member();
    final UUID key = member.id();
    final MembershipRecord oldRecord = recordMap.get(key);
    if (oldRecord == null) {
      recordMap.put(key, record);
      remoteMembers.add(member);
      emitMemberAction(ADD_MEMBER, member);
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

    if (record.incarnation() > oldRecord.incarnation() || record.status() != ALIVE) {
      update(record, record.status());
    }
  }

  public void put(Member member, MemberStatus status) {
    final MembershipRecord record = recordMap.get(member.id());
    if (record == null) {
      return;
    }

    final String namespace = record.namespace();
    if (!localRecord.namespace().equals(namespace)) {
      return;
    }

    if (record.status() != status) {
      update(record, status);
      emitGossip(record);
    }
  }

  public void forEach(Consumer<MembershipRecord> consumer) {
    recordMap.values().forEach(consumer);
  }

  public int size() {
    return recordMap.size();
  }

  private void emitMemberAction(MemberActionType actionType, Member member) {
    messageTx.transmit(
        1, memberActionCodec.encode(actionType, member), 0, memberActionCodec.encodedLength());
  }

  private void emitGossip(MembershipRecord record) {
    final MutableDirectBuffer buffer = membershipRecordCodec.encode(record);
    final int encodedLength = membershipRecordCodec.encodedLength();
    messageTx.transmit(
        1,
        gossipMessageCodec.encodeOutputMessage(buffer, 0, encodedLength),
        0,
        gossipMessageCodec.encodedLength());
  }

  private void update(MembershipRecord record, MemberStatus status) {
    final UUID key = record.member().id();

    recordMap.put(key, record.status(status));

    if (status == ALIVE) {
      cancelTimer(key);
    } else {
      scheduleTimer(key);
    }
  }

  private void cancelTimer(UUID key) {
    final long timerId = timerIdByMemberId.removeKey(key);
    memberIdByTimerId.remove(timerId);
    timerInvoker.cancelTimer(timerId);
  }

  private void scheduleTimer(UUID key) {
    long suspicionTimeout =
        ClusterMath.suspicionTimeout(suspicionMult, recordMap.size(), pingInterval);
    final long deadline = epochClock.time() + suspicionTimeout;
    final long timerId = timerInvoker.scheduleTimer(deadline);
    timerIdByMemberId.put(key, timerId);
    memberIdByTimerId.put(timerId, key);
  }

  private void onTimerExpiry(long timerId) {
    final UUID memberId = memberIdByTimerId.remove(timerId);
    if (memberId != null) {
      timerIdByMemberId.removeKey(memberId);
      final MembershipRecord record = recordMap.remove(memberId);
      if (record != null) {
        final Member member = record.member();
        removeFromRemoteMembers(member);
        emitMemberAction(REMOVE_MEMBER, member);
        // TODO: emit to external clients of the lib - MembershipEvent(type=REMOVED)
      }
    }
  }

  private void removeFromRemoteMembers(Member member) {
    final int index = remoteMembers.indexOf(member);
    if (index != -1) {
      ArrayListUtil.fastUnorderedRemove(remoteMembers, index);
    }
  }
}
