package io.scalecube.cluster2.gossip;

import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

public class GossipState {

  /** Target gossip. */
  private final Gossip gossip;

  /** Local gossip period when gossip was received for the first time. */
  private final long infectionPeriod;

  /** Set of member IDs this gossip was received from. */
  private final Set<UUID> infectedSet = new HashSet<>();

  public GossipState(Gossip gossip, long infectionPeriod) {
    this.gossip = gossip;
    this.infectionPeriod = infectionPeriod;
  }

  public Gossip gossip() {
    return gossip;
  }

  public long infectionPeriod() {
    return infectionPeriod;
  }

  public void addToInfected(UUID memberId) {
    infectedSet.add(memberId);
  }

  public boolean isInfected(UUID memberId) {
    return infectedSet.contains(memberId);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GossipState.class.getSimpleName() + "[", "]")
        .add("gossip=" + gossip)
        .add("infectionPeriod=" + infectionPeriod)
        .add("infectedSet=" + infectedSet)
        .toString();
  }
}
