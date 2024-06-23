package io.scalecube.cluster2.gossip;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;

public class Gossip {

  private UUID gossiperId;
  private long sequenceId; // incremented counter
  private byte[] message;

  public Gossip(UUID gossiperId, byte[] message, long sequenceId) {
    this.gossiperId = gossiperId;
    this.message = message;
    this.sequenceId = sequenceId;
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

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass()) {
      return false;
    }
    Gossip gossip = (Gossip) that;
    return sequenceId == gossip.sequenceId
        && Objects.equals(gossiperId, gossip.gossiperId)
        && Objects.equals(message, gossip.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(gossiperId, message, sequenceId);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Gossip.class.getSimpleName() + "[", "]")
        .add("gossiperId='" + gossiperId + "'")
        .add("message=" + message)
        .add("sequenceId=" + sequenceId)
        .toString();
  }
}
