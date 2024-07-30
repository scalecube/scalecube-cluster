package io.scalecube.cluster2.membership;

import static io.scalecube.cluster2.sbe.MemberStatus.ALIVE;

import io.scalecube.cluster2.ClusterMath;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.TimerInvoker;
import io.scalecube.cluster2.gossip.GossipMessageCodec;
import io.scalecube.cluster2.payload.PayloadCodec;
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
  private final PayloadCodec payloadCodec = new PayloadCodec();
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
    if (!localRecord.namespace().equals(record.namespace())) {
      return;
    }

    final Member member = record.member();
    final UUID key = member.id();
    final MembershipRecord currRecord = recordMap.get(key);
    final MemberStatus status = record.status();

    if (currRecord == null) {
      recordMap.put(key, record);
      remoteMembers.add(member);
      emitAddMember(member);
      emitPayloadGenerationUpdated(record);
      return;
    }

    tryUpdatePayloadGeneration(record);

    if (record.incarnation() < currRecord.incarnation()) {
      return;
    }

    if (localMember.equals(member) && status != ALIVE) {
      localRecord.status(status).incarnation(localRecord.incarnation() + 1);
      emitGossip(localRecord);
      return;
    }

    if (record.incarnation() > currRecord.incarnation() || status != ALIVE) {
      updateStatus(record, status);
    }
  }

  public void put(Member member, MemberStatus status) {
    final MembershipRecord record = recordMap.get(member.id());
    if (record != null && record.status() != status) {
      updateStatus(record, status);
      emitGossip(record);
    }
  }

  public void forEach(Consumer<MembershipRecord> consumer) {
    recordMap.values().forEach(consumer);
  }

  public int size() {
    return recordMap.size();
  }

  public void updatePayloadGeneration(long generation, int payloadLength) {
    localRecord.generation(generation).payloadLength(payloadLength);
  }

  private void tryUpdatePayloadGeneration(MembershipRecord record) {
    final Member member = record.member();
    final UUID key = member.id();
    final MembershipRecord currRecord = recordMap.get(key);
    if (currRecord.generation() < record.generation()) {
      emitPayloadGenerationUpdated(record);
    }
  }

  private void emitPayloadGenerationUpdated(MembershipRecord record) {
    final UUID memberId = record.member().id();
    final long generation = record.generation();
    final int payloadLength = record.payloadLength();
    messageTx.transmit(
        1,
        payloadCodec.encodePayloadGenerationUpdated(memberId, generation, payloadLength),
        0,
        payloadCodec.encodedLength());
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

  private void updateStatus(MembershipRecord record, MemberStatus status) {
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
        emitRemoveMember(member);
      }
    }
  }

  private void removeFromRemoteMembers(Member member) {
    final int index = remoteMembers.indexOf(member);
    if (index != -1) {
      ArrayListUtil.fastUnorderedRemove(remoteMembers, index);
    }
  }

  private void emitAddMember(Member member) {
    messageTx.transmit(
        1, memberActionCodec.encodeAddMember(member), 0, memberActionCodec.encodedLength());
  }

  private void emitRemoveMember(Member member) {
    messageTx.transmit(
        1, memberActionCodec.encodeRemoveMember(member), 0, memberActionCodec.encodedLength());
  }
}
