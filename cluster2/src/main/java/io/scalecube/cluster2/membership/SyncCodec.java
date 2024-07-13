package io.scalecube.cluster2.membership;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.sbe.SyncAckEncoder;
import io.scalecube.cluster2.sbe.SyncEncoder;
import org.agrona.MutableDirectBuffer;

public class SyncCodec extends AbstractCodec {

  private final SyncEncoder syncEncoder = new SyncEncoder();
  private final SyncAckEncoder syncAckEncoder = new SyncAckEncoder();
  private final MembershipRecordCodec membershipRecordCodec = new MembershipRecordCodec();

  public SyncCodec() {}

  public MutableDirectBuffer encodeSync(
      int period, String from, MembershipRecord membershipRecord) {
    encodedLength = 0;

    syncEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    syncEncoder.period(period);
    syncEncoder.from(from);
    syncEncoder.putMembershipRecord(
        membershipRecordCodec.encode(membershipRecord), 0, membershipRecordCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + syncEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodeSyncAck(
      int period, String from, MembershipRecord membershipRecord) {
    encodedLength = 0;

    syncAckEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    syncAckEncoder.period(period);
    syncAckEncoder.from(from);
    syncAckEncoder.putMembershipRecord(
        membershipRecordCodec.encode(membershipRecord), 0, membershipRecordCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + syncAckEncoder.encodedLength();
    return encodedBuffer;
  }
}
