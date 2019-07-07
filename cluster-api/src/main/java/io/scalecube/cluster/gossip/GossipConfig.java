package io.scalecube.cluster.gossip;

import reactor.core.Exceptions;

public final class GossipConfig implements Cloneable {

  // Default settings for LAN cluster
  public static final long DEFAULT_GOSSIP_INTERVAL = 200;
  public static final int DEFAULT_GOSSIP_FANOUT = 3;
  public static final int DEFAULT_GOSSIP_REPEAT_MULT = 3;

  // Default settings for WAN cluster (overrides default/LAN settings)
  public static final int DEFAULT_WAN_GOSSIP_FANOUT = 4;

  // Default settings for local cluster working via loopback interface (overrides default/LAN
  // settings)
  public static final int DEFAULT_LOCAL_GOSSIP_REPEAT_MULT = 2;
  public static final int DEFAULT_LOCAL_GOSSIP_INTERVAL = 100;

  private int gossipFanout = DEFAULT_GOSSIP_FANOUT;
  private long gossipInterval = DEFAULT_GOSSIP_INTERVAL;
  private int gossipRepeatMult = DEFAULT_GOSSIP_REPEAT_MULT;

  public GossipConfig() {}

  public static GossipConfig defaultConfig() {
    return new GossipConfig();
  }

  /**
   * Creates {@code GossipConfig} with default settings for cluster on LAN network.
   *
   * @return new {@code GossipConfig}
   */
  public static GossipConfig defaultLanConfig() {
    return defaultConfig();
  }

  /**
   * Creates {@code GossipConfig} with default settings for cluster on WAN network.
   *
   * @return new {@code GossipConfig}
   */
  public static GossipConfig defaultWanConfig() {
    return defaultConfig().gossipFanout(DEFAULT_WAN_GOSSIP_FANOUT);
  }

  /**
   * Creates {@code GossipConfig} with default settings for cluster on local loopback interface.
   *
   * @return new {@code GossipConfig}
   */
  public static GossipConfig defaultLocalConfig() {
    return defaultConfig()
        .gossipRepeatMult(DEFAULT_LOCAL_GOSSIP_REPEAT_MULT)
        .gossipInterval(DEFAULT_LOCAL_GOSSIP_INTERVAL);
  }

  /**
   * Sets gossipFanout.
   *
   * @param gossipFanout gossip fanout
   * @return new {@code GossipConfig}
   */
  public GossipConfig gossipFanout(int gossipFanout) {
    GossipConfig g = clone();
    g.gossipFanout = gossipFanout;
    return g;
  }

  public int gossipFanout() {
    return gossipFanout;
  }

  /**
   * Sets gossipInterval.
   *
   * @param gossipInterval gossip interval
   * @return new {@code GossipConfig}
   */
  public GossipConfig gossipInterval(long gossipInterval) {
    GossipConfig g = clone();
    g.gossipInterval = gossipInterval;
    return g;
  }

  public long gossipInterval() {
    return gossipInterval;
  }

  /**
   * Sets gossipRepeatMult.
   *
   * @param gossipRepeatMult gossip repeat multiplier
   * @return new {@code GossipConfig}
   */
  public GossipConfig gossipRepeatMult(int gossipRepeatMult) {
    GossipConfig g = clone();
    g.gossipRepeatMult = gossipRepeatMult;
    return g;
  }

  public int gossipRepeatMult() {
    return gossipRepeatMult;
  }

  @Override
  public GossipConfig clone() {
    try {
      return (GossipConfig) super.clone();
    } catch (CloneNotSupportedException e) {
      throw Exceptions.propagate(e);
    }
  }

  @Override
  public String toString() {
    return "GossipConfig{"
        + "gossipFanout="
        + gossipFanout
        + ", gossipInterval="
        + gossipInterval
        + ", gossipRepeatMult="
        + gossipRepeatMult
        + '}';
  }
}
