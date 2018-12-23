package io.scalecube.cluster.gossip;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Gossip request which be transmitted through the network, contains list of gossips. */
final class GossipRequest {

  private List<Gossip> gossips;
  private String from;

  /** Instantiates empty gossip request for deserialization purpose. */
  GossipRequest() {}

  public GossipRequest(Gossip gossip, String from) {
    this(Collections.singletonList(gossip), from);
  }

  public GossipRequest(List<Gossip> gossips, String from) {
    this.gossips = new ArrayList<>(gossips);
    this.from = from;
  }

  public List<Gossip> gossips() {
    return gossips;
  }

  public String from() {
    return from;
  }

  @Override
  public String toString() {
    return "GossipRequest{gossips=" + gossips + ", from=" + from + '}';
  }
}
