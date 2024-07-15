package io.scalecube.cluster2.gossip;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.sbe.GossipInputMessageEncoder;
import io.scalecube.cluster2.sbe.GossipOutputMessageEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class GossipMessageCodec extends AbstractCodec {

  private final GossipOutputMessageEncoder gossipOutputMessageEncoder =
      new GossipOutputMessageEncoder();
  private final GossipInputMessageEncoder gossipInputMessageEncoder =
      new GossipInputMessageEncoder();

  public GossipMessageCodec() {}

  public MutableDirectBuffer encodeOutputMessage(DirectBuffer buffer, int offset, int length) {
    encodedLength = 0;

    gossipOutputMessageEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    gossipOutputMessageEncoder.putMessage(buffer, offset, length);

    encodedLength = headerEncoder.encodedLength() + gossipOutputMessageEncoder.encodedLength();
    return encodedBuffer;
  }

  public MutableDirectBuffer encodeInputMessage(DirectBuffer buffer, int offset, int length) {
    encodedLength = 0;

    gossipInputMessageEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    gossipInputMessageEncoder.putMessage(buffer, offset, length);

    encodedLength = headerEncoder.encodedLength() + gossipInputMessageEncoder.encodedLength();
    return encodedBuffer;
  }
}
