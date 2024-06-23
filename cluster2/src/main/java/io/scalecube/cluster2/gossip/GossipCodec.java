package io.scalecube.cluster2.gossip;

import io.scalecube.cluster2.AbstractCodec;
import java.util.function.Consumer;
import org.agrona.concurrent.UnsafeBuffer;

public class GossipCodec extends AbstractCodec {

  public GossipCodec() {}

  // Decode

  public Gossip gossip(Consumer<UnsafeBuffer> consumer) {
    consumer.accept(unsafeBuffer);

    // TODO

    return new Gossip(null, null, 1);
  }
}
