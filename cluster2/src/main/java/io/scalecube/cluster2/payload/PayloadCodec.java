package io.scalecube.cluster2.payload;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.PayloadChunkRequestEncoder;
import io.scalecube.cluster2.sbe.PayloadChunkResponseEncoder;
import io.scalecube.cluster2.sbe.PayloadGenerationUpdatedEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class PayloadCodec extends AbstractCodec {

  private final PayloadGenerationUpdatedEncoder payloadGenerationUpdatedEncoder =
      new PayloadGenerationUpdatedEncoder();
  private final PayloadChunkRequestEncoder payloadChunkRequestEncoder =
      new PayloadChunkRequestEncoder();
  private final PayloadChunkResponseEncoder payloadChunkResponseEncoder =
      new PayloadChunkResponseEncoder();
  private final MemberCodec memberCodec = new MemberCodec();

  public PayloadCodec() {}

  public MutableDirectBuffer encodePayloadGenerationUpdated(long generation, int payloadLength) {
    encodedLength = 0;

    payloadGenerationUpdatedEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    payloadGenerationUpdatedEncoder.generation(generation);
    payloadGenerationUpdatedEncoder.payloadLength(payloadLength);

    encodedLength = headerEncoder.encodedLength() + payloadGenerationUpdatedEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodePayloadChunkRequest(long generation, long payloadOffset) {
    encodedLength = 0;

    payloadChunkRequestEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    payloadChunkRequestEncoder.generation(generation);
    payloadChunkRequestEncoder.payloadOffset(payloadOffset);

    encodedLength = headerEncoder.encodedLength() + payloadChunkRequestEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodePayloadChunkResponse(
      long generation, long payloadOffset, DirectBuffer src, int srcOffset, int length) {
    encodedLength = 0;

    payloadChunkResponseEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    payloadChunkRequestEncoder.generation(generation);
    payloadChunkResponseEncoder.payloadOffset(payloadOffset);
    payloadChunkResponseEncoder.putChunk(src, srcOffset, length);

    encodedLength = headerEncoder.encodedLength() + payloadChunkResponseEncoder.encodedLength();
    return encodedBuffer;
  }
}
