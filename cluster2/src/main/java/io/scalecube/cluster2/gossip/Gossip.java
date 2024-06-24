package io.scalecube.cluster2.gossip;

import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

public class Gossip {

  /** Gossip id, local member id. */
  private final UUID gossiperId;

  /** Incremented counter */
  private final long sequenceId;

  /** Gossip message. */
  private final byte[] message;

  /** Local gossip period when gossip was received for the first time. */
  private final long infectionPeriod;

  /** Set of member IDs this gossip was received from. */
  private final Set<UUID> infectedSet = new HashSet<>();

  /**
   * Constructor.
   *
   * @param gossiperId gossiperId.
   * @param sequenceId sequenceId.
   * @param message message.
   * @param infectionPeriod infectionPeriod.
   */
  public Gossip(UUID gossiperId, long sequenceId, byte[] message, long infectionPeriod) {
    this.gossiperId = gossiperId;
    this.sequenceId = sequenceId;
    this.message = message;
    this.infectionPeriod = infectionPeriod;
  }

  public String gossipId() {
    return gossiperId + "-" + sequenceId;
  }

  public UUID gossiperId() {
    return gossiperId;
  }

  public byte[] message() {
    return message;
  }

  public long sequenceId() {
    return sequenceId;
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
    return new StringJoiner(", ", Gossip.class.getSimpleName() + "[", "]")
        .add("gossiperId=" + gossiperId)
        .add("sequenceId=" + sequenceId)
        .add("message[" + message.length + "]")
        .add("infectionPeriod=" + infectionPeriod)
        .add("infectedSet[" + infectedSet.size() + "]")
        .toString();
  }
}
