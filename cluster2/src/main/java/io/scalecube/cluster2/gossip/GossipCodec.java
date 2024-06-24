package io.scalecube.cluster2.gossip;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.UUIDCodec;
import io.scalecube.cluster2.sbe.GossipEncoder;
import org.agrona.MutableDirectBuffer;

public class GossipCodec extends AbstractCodec {

  private final GossipEncoder gossipEncoder = new GossipEncoder();

  public GossipCodec() {}

  public MutableDirectBuffer encode(Gossip gossip) {
    encodedLength = 0;

    gossipEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    UUIDCodec.encode(gossip.gossiperId(), gossipEncoder.gossiperId());
    gossipEncoder.sequenceId(gossip.sequenceId());
    gossipEncoder.putMessage(gossip.message(), 0, gossip.message().length);

    encodedLength = headerEncoder.encodedLength() + gossipEncoder.encodedLength();
    return encodedBuffer;
  }
}
