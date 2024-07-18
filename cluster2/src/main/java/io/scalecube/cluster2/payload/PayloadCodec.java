package io.scalecube.cluster2.payload;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.sbe.GenerationGoneEncoder;
import io.scalecube.cluster2.sbe.GenerationRequestEncoder;
import io.scalecube.cluster2.sbe.GenerationResponseEncoder;
import io.scalecube.cluster2.sbe.PayloadChunkRequestEncoder;
import io.scalecube.cluster2.sbe.PayloadChunkResponseEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class PayloadCodec extends AbstractCodec {

  private final GenerationRequestEncoder generationRequestEncoder = new GenerationRequestEncoder();
  private final GenerationResponseEncoder generationResponseEncoder =
      new GenerationResponseEncoder();
  private final GenerationGoneEncoder generationGoneEncoder = new GenerationGoneEncoder();
  private final PayloadChunkRequestEncoder payloadChunkRequestEncoder =
      new PayloadChunkRequestEncoder();
  private final PayloadChunkResponseEncoder payloadChunkResponseEncoder =
      new PayloadChunkResponseEncoder();

  public PayloadCodec() {}

  public MutableDirectBuffer encodeGenerationRequest(long genId) {
    encodedLength = 0;

    generationRequestEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    generationRequestEncoder.genId(genId);

    encodedLength = headerEncoder.encodedLength() + generationRequestEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodeGenerationResponse(long genId, long genLength) {
    encodedLength = 0;

    generationResponseEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    generationResponseEncoder.genId(genId);
    generationResponseEncoder.genLength(genLength);

    encodedLength = headerEncoder.encodedLength() + generationResponseEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodeGenerationGone(long genId) {
    encodedLength = 0;

    generationGoneEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    generationGoneEncoder.genId(genId);

    encodedLength = headerEncoder.encodedLength() + generationGoneEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodePayloadChunkRequest(long genId, long payloadOffset) {
    encodedLength = 0;

    payloadChunkRequestEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    payloadChunkRequestEncoder.genId(genId);
    payloadChunkRequestEncoder.payloadOffset(payloadOffset);

    encodedLength = headerEncoder.encodedLength() + payloadChunkRequestEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodePayloadChunkResponse(
      long genId, long payloadOffset, DirectBuffer src, int srcOffset, int length) {
    encodedLength = 0;

    payloadChunkResponseEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    payloadChunkResponseEncoder.genId(genId);
    payloadChunkResponseEncoder.payloadOffset(payloadOffset);
    payloadChunkResponseEncoder.putChunk(src, srcOffset, length);

    encodedLength = headerEncoder.encodedLength() + payloadChunkResponseEncoder.encodedLength();
    return encodedBuffer;
  }
}
