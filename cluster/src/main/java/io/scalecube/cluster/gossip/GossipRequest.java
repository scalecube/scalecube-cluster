package io.scalecube.cluster.gossip;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/** Gossip request which be transmitted through the network, contains list of gossips. */
final class GossipRequest implements Externalizable {

  private static final long serialVersionUID = 1L;

  private List<Gossip> gossips;
  private String from;

  public GossipRequest() {}

  public GossipRequest(Gossip gossip, String from) {
    this(Collections.singletonList(gossip), from);
  }

  public GossipRequest(List<Gossip> gossips, String from) {
    Objects.requireNonNull(gossips);
    Objects.requireNonNull(from);
    this.gossips = Collections.unmodifiableList(new ArrayList<>(gossips));
    this.from = from;
  }

  public List<Gossip> gossips() {
    return gossips;
  }

  public String from() {
    return from;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // gossips
    out.writeInt(gossips.size());
    for (Gossip gossip : gossips) {
      out.writeObject(gossip);
    }
    // from
    out.writeUTF(from);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // gossips
    int gossipsSize = in.readInt();
    List<Gossip> gossips = new ArrayList<>(gossipsSize);
    for (int i = 0; i < gossipsSize; i++) {
      gossips.add((Gossip) in.readObject());
    }
    this.gossips = Collections.unmodifiableList(gossips);
    // from
    from = in.readUTF();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GossipRequest.class.getSimpleName() + "[", "]")
        .add("gossips=" + gossips)
        .add("from='" + from + "'")
        .toString();
  }
}
