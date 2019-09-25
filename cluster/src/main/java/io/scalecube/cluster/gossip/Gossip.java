package io.scalecube.cluster.gossip;

import io.scalecube.cluster.transport.api.Message;
import java.util.Objects;
import java.util.StringJoiner;

/** Data model for gossip, include gossip id, qualifier and object need to disseminate. */
final class Gossip {

  private String gossiperId;
  private Message message;
  // incremented counter
  private long sequenceId;

  /** Instantiates empty gossip for deserialization purpose. */
  Gossip() {}

  public Gossip(String gossiperId, Message message, long sequenceId) {
    this.gossiperId = Objects.requireNonNull(gossiperId);
    this.message = Objects.requireNonNull(message);
    this.sequenceId = sequenceId;
  }

  public String gossipId() {
    return gossiperId + "-" + sequenceId;
  }

  public String gossiperId() {
    return gossiperId;
  }

  public Message message() {
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
