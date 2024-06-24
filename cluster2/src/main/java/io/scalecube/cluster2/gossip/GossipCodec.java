package io.scalecube.cluster2.gossip;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.UUIDCodec;
import io.scalecube.cluster2.sbe.GossipEncoder;
import org.agrona.MutableDirectBuffer;

public class GossipCodec extends AbstractCodec {

  private final GossipEncoder gossipEncoder = new GossipEncoder();

  public GossipCodec() {}

  public MutableDirectBuffer encode(GossipState gossipState) {
    encodedLength = 0;

    gossipEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    UUIDCodec.encode(gossipState.gossiperId(), gossipEncoder.gossiperId());
    gossipEncoder.sequenceId(gossipState.sequenceId());
    gossipEncoder.putMessage(gossipState.message(), 0, gossipState.message().length);

    encodedLength = headerEncoder.encodedLength() + gossipEncoder.encodedLength();
    return encodedBuffer;
  }
}
