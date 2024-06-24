package io.scalecube.cluster2.gossip;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.UUIDCodec;
import io.scalecube.cluster2.sbe.GossipRequestEncoder;
import java.util.UUID;
import org.agrona.MutableDirectBuffer;

public class GossipRequestCodec extends AbstractCodec {

  private final GossipRequestEncoder gossipRequestEncoder = new GossipRequestEncoder();
  private final GossipCodec gossipCodec = new GossipCodec();

  public GossipRequestCodec() {}

  public MutableDirectBuffer encode(UUID from, GossipState gossipState) {
    encodedLength = 0;

    gossipRequestEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    UUIDCodec.encode(from, gossipRequestEncoder.from());
    gossipRequestEncoder.putGossip(gossipCodec.encode(gossipState), 0, gossipCodec.encodedLength());

    encodedLength = headerEncoder.encodedLength() + gossipRequestEncoder.encodedLength();
    return encodedBuffer;
  }
}
