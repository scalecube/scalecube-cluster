package io.scalecube.cluster2.membership;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.sbe.SyncAckEncoder;
import io.scalecube.cluster2.sbe.SyncEncoder;
import org.agrona.MutableDirectBuffer;

public class SyncCodec extends AbstractCodec {

  private static final byte[] EMPTY_BYTES = new byte[0];

  private final SyncEncoder syncEncoder = new SyncEncoder();
  private final SyncAckEncoder syncAckEncoder = new SyncAckEncoder();
  private final MembershipRecordCodec membershipRecordCodec = new MembershipRecordCodec();

  public SyncCodec() {}

  public MutableDirectBuffer encodeSync(long period, String from, MembershipRecord record) {
    encodedLength = 0;

    syncEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    syncEncoder.period(period);
    syncEncoder.from(from);

    if (record != null) {
      syncEncoder.putMembershipRecord(
          membershipRecordCodec.encode(record), 0, membershipRecordCodec.encodedLength());
    } else {
      syncEncoder.putMembershipRecord(EMPTY_BYTES, 0, 0);
    }

    encodedLength = headerEncoder.encodedLength() + syncEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodeSyncAck(long period, MembershipRecord record) {
    encodedLength = 0;

    syncAckEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    syncAckEncoder.period(period);
    syncAckEncoder.putMembershipRecord(
        membershipRecordCodec.encode(record), 0, membershipRecordCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + syncAckEncoder.encodedLength();
    return encodedBuffer;
  }
}
