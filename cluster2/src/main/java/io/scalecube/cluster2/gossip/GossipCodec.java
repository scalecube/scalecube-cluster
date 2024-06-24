package io.scalecube.cluster2.gossip;

import static io.scalecube.cluster2.UUIDCodec.uuid;

import io.scalecube.cluster2.AbstractCodec;
import io.scalecube.cluster2.UUIDCodec;
import io.scalecube.cluster2.sbe.GossipDecoder;
import io.scalecube.cluster2.sbe.GossipEncoder;
import java.util.UUID;
import java.util.function.Consumer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class GossipCodec extends AbstractCodec {

  private final GossipEncoder gossipEncoder = new GossipEncoder();
  private final GossipDecoder gossipDecoder = new GossipDecoder();

  public GossipCodec() {}

  // Encode

  public MutableDirectBuffer encode(Gossip gossip) {
    encodedLength = 0;

    gossipEncoder.wrapAndApplyHeader(encodedBuffer, 0, headerEncoder);
    UUIDCodec.encode(gossip.gossiperId(), gossipEncoder.gossiperId());
    gossipEncoder.sequenceId(gossip.sequenceId());
    gossipEncoder.putMessage(gossip.message(), 0, gossip.message().length);

    encodedLength = headerEncoder.encodedLength() + gossipEncoder.encodedLength();
    return encodedBuffer;
  }

  // Decode

  public Gossip gossip(Consumer<UnsafeBuffer> consumer) {
    consumer.accept(unsafeBuffer);

    gossipDecoder.wrapAndApplyHeader(unsafeBuffer, 0, headerDecoder);

    final UUID gossiperId = uuid(gossipDecoder.gossiperId());
    if (gossiperId == null) {
      return null;
    }

    final long sequenceId = gossipDecoder.sequenceId();

    final int messageLength = gossipDecoder.messageLength();
    final byte[] message = new byte[messageLength];
    gossipDecoder.getMessage(message, 0, messageLength);

    return new Gossip(gossiperId, sequenceId, message);
  }
}
