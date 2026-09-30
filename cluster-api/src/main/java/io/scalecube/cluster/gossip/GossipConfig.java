package io.scalecube.cluster.gossip;

import java.util.Properties;
import java.util.StringJoiner;

public final class GossipConfig {

  public static final int DEFAULT_GOSSIP_FANOUT = 3;
  public static final long DEFAULT_GOSSIP_INTERVAL = 200;
  public static final int DEFAULT_GOSSIP_REPEAT_MULT = 3;
  public static final int DEFAULT_GOSSIP_SEGMENTATION_THRESHOLD = 1000;

  public static final String GOSSIP_FANOUT_PROP_NAME = "scalecube.cluster.gossip.gossipFanout";
  public static final String GOSSIP_INTERVAL_PROP_NAME = "scalecube.cluster.gossip.gossipInterval";
  public static final String GOSSIP_REPEAT_MULT_PROP_NAME =
      "scalecube.cluster.gossip.gossipRepeatMult";
  public static final String GOSSIP_SEGMENTATION_THRESHOLD_PROP_NAME =
      "scalecube.cluster.gossip.gossipSegmentationThreshold";

  private int gossipFanout;
  private long gossipInterval;
  private int gossipRepeatMult;
  private int gossipSegmentationThreshold;

  public GossipConfig() {
    this(System.getProperties());
  }

  public GossipConfig(Properties properties) {
    gossipFanout(properties);
    gossipInterval(properties);
    gossipRepeatMult(properties);
    gossipSegmentationThreshold(properties);
  }

  private static String getProperty(Properties properties, String name) {
    final var value = properties.getProperty(name);
    return "@null".equals(value) ? null : value;
  }

  private static int getProperty(Properties properties, String name, int defaultValue) {
    final var value = getProperty(properties, name);
    return value != null ? Integer.parseInt(value) : defaultValue;
  }

  private static long getProperty(Properties properties, String name, long defaultValue) {
    final var value = getProperty(properties, name);
    return value != null ? Long.parseLong(value) : defaultValue;
  }

  public int gossipFanout() {
    return gossipFanout;
  }

  public GossipConfig gossipFanout(int gossipFanout) {
    this.gossipFanout = gossipFanout;
    return this;
  }

  public GossipConfig gossipFanout(Properties properties) {
    return gossipFanout(getProperty(properties, GOSSIP_FANOUT_PROP_NAME, DEFAULT_GOSSIP_FANOUT));
  }

  public long gossipInterval() {
    return gossipInterval;
  }

  public GossipConfig gossipInterval(long gossipInterval) {
    this.gossipInterval = gossipInterval;
    return this;
  }

  public GossipConfig gossipInterval(Properties properties) {
    return gossipInterval(
        getProperty(properties, GOSSIP_INTERVAL_PROP_NAME, DEFAULT_GOSSIP_INTERVAL));
  }

  public int gossipRepeatMult() {
    return gossipRepeatMult;
  }

  public GossipConfig gossipRepeatMult(int gossipRepeatMult) {
    this.gossipRepeatMult = gossipRepeatMult;
    return this;
  }

  public GossipConfig gossipRepeatMult(Properties properties) {
    return gossipRepeatMult(
        getProperty(properties, GOSSIP_REPEAT_MULT_PROP_NAME, DEFAULT_GOSSIP_REPEAT_MULT));
  }

  /**
   * A threshold for received gossip id intervals. If number of intervals is more than threshold
   * then warning will be raised, this mean that node losing network frequently for a long time.
   *
   * <p>For example if we received gossip with id 1,2 and 5 then we will have 2 intervals [1, 2],
   * [5, 5].
   *
   * @return gossip segmentation threshold
   */
  public int gossipSegmentationThreshold() {
    return gossipSegmentationThreshold;
  }

  public GossipConfig gossipSegmentationThreshold(int gossipSegmentationThreshold) {
    this.gossipSegmentationThreshold = gossipSegmentationThreshold;
    return this;
  }

  public GossipConfig gossipSegmentationThreshold(Properties properties) {
    return gossipSegmentationThreshold(
        getProperty(
            properties,
            GOSSIP_SEGMENTATION_THRESHOLD_PROP_NAME,
            DEFAULT_GOSSIP_SEGMENTATION_THRESHOLD));
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GossipConfig.class.getSimpleName() + "[", "]")
        .add("gossipFanout=" + gossipFanout)
        .add("gossipInterval=" + gossipInterval)
        .add("gossipRepeatMult=" + gossipRepeatMult)
        .add("gossipSegmentationThreshold=" + gossipSegmentationThreshold)
        .toString();
  }
}
