package io.scalecube.cluster2.payload;

import static io.scalecube.cluster2.UUIDCodec.encodeUUID;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.PayloadChunkRequestEncoder;
import io.scalecube.cluster2.sbe.PayloadChunkResponseEncoder;
import io.scalecube.cluster2.sbe.PayloadGenerationEventEncoder;
import java.util.UUID;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class PayloadCodec extends AbstractCodec {

  private final PayloadGenerationEventEncoder payloadGenerationEventEncoder =
      new PayloadGenerationEventEncoder();
  private final PayloadChunkRequestEncoder payloadChunkRequestEncoder =
      new PayloadChunkRequestEncoder();
  private final PayloadChunkResponseEncoder payloadChunkResponseEncoder =
      new PayloadChunkResponseEncoder();
  private final MemberCodec memberCodec = new MemberCodec();

  public PayloadCodec() {}

  public MutableDirectBuffer encodePayloadGenerationEvent(UUID memberId, int payloadLength) {
    encodedLength = 0;

    payloadGenerationEventEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    encodeUUID(memberId, payloadGenerationEventEncoder.memberId());
    payloadGenerationEventEncoder.payloadLength(payloadLength);

    encodedLength = headerEncoder.encodedLength() + payloadGenerationEventEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodePayloadChunkRequest(Member from, int payloadOffset) {
    encodedLength = 0;

    payloadChunkRequestEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    payloadChunkRequestEncoder.payloadOffset(payloadOffset);
    payloadChunkRequestEncoder.putFrom(memberCodec.encode(from), 0, memberCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + payloadChunkRequestEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodePayloadChunkResponse(
      Member from, int payloadOffset, DirectBuffer chunk, int chunkOffset, int chunkLength) {
    encodedLength = 0;

    payloadChunkResponseEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    payloadChunkResponseEncoder.payloadOffset(payloadOffset);
    payloadChunkResponseEncoder.putFrom(memberCodec.encode(from), 0, memberCodec.encodedLength());
    payloadChunkResponseEncoder.putChunk(chunk, chunkOffset, chunkLength);

    encodedLength = headerEncoder.encodedLength() + payloadChunkResponseEncoder.encodedLength();
    return encodedBuffer;
  }
}
