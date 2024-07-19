package io.scalecube.cluster2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import reactor.core.Exceptions;

public class ClusterConfig implements Cloneable {

  // Default settings for LAN cluster
  public static final int DEFAULT_SUSPICION_MULT = 5;
  public static final int DEFAULT_SYNC_INTERVAL = 30_000;

  // Default settings for WAN cluster (overrides default/LAN settings)
  public static final int DEFAULT_WAN_SUSPICION_MULT = 6;
  public static final int DEFAULT_WAN_SYNC_INTERVAL = 60_000;

  // Default settings for local cluster working via loopback interface (overrides default/LAN
  // settings)
  public static final int DEFAULT_LOCAL_SUSPICION_MULT = 3;
  public static final int DEFAULT_LOCAL_SYNC_INTERVAL = 3_000;

  // Default settings for LAN cluster
  public static final int DEFAULT_PING_INTERVAL = 1_000;
  public static final int DEFAULT_PING_REQ_MEMBERS = 3;

  // Default settings for WAN cluster (overrides default/LAN settings)
  public static final int DEFAULT_WAN_PING_INTERVAL = 5_000;

  // Default settings for local cluster working via loopback interface (overrides default/LAN
  // settings)
  public static final int DEFAULT_LOCAL_PING_INTERVAL = 1_000;
  public static final int DEFAULT_LOCAL_PING_REQ_MEMBERS = 1;

  // Default settings for LAN cluster
  public static final long DEFAULT_GOSSIP_INTERVAL = 200;
  public static final int DEFAULT_GOSSIP_FANOUT = 3;
  public static final int DEFAULT_GOSSIP_REPEAT_MULT = 3;
  public static final int GOSSIP_SEGMENTATION_THRESHOLD = 1000;

  // Default settings for WAN cluster (overrides default/LAN settings)
  public static final int DEFAULT_WAN_GOSSIP_FANOUT = 4;

  // Default settings for local cluster working via loopback interface (overrides default/LAN
  // settings)
  public static final int DEFAULT_LOCAL_GOSSIP_REPEAT_MULT = 2;
  public static final int DEFAULT_LOCAL_GOSSIP_INTERVAL = 100;

  // Default ping interval for payload protocol
  public static final int DEFAULT_PAYLOAD_INTERVAL = 200;

  private List<String> seedMembers = Collections.emptyList();
  private int syncInterval = DEFAULT_SYNC_INTERVAL;
  private int suspicionMult = DEFAULT_SUSPICION_MULT;
  private String namespace = "default";
  private int pingInterval = DEFAULT_PING_INTERVAL;
  private int pingReqMembers = DEFAULT_PING_REQ_MEMBERS;
  private int gossipFanout = DEFAULT_GOSSIP_FANOUT;
  private long gossipInterval = DEFAULT_GOSSIP_INTERVAL;
  private int gossipRepeatMult = DEFAULT_GOSSIP_REPEAT_MULT;
  private int gossipSegmentationThreshold = GOSSIP_SEGMENTATION_THRESHOLD;
  private int payloadInterval = DEFAULT_PAYLOAD_INTERVAL;

  public ClusterConfig() {}

  public List<String> seedMembers() {
    return seedMembers;
  }

  public ClusterConfig seedMembers(String... seedMembers) {
    return seedMembers(Arrays.asList(seedMembers));
  }

  public ClusterConfig seedMembers(List<String> seedMembers) {
    this.seedMembers = Collections.unmodifiableList(new ArrayList<>(seedMembers));
    return this;
  }

  public int syncInterval() {
    return syncInterval;
  }

  public ClusterConfig syncInterval(int syncInterval) {
    this.syncInterval = syncInterval;
    return this;
  }

  public int suspicionMult() {
    return suspicionMult;
  }

  public ClusterConfig suspicionMult(int suspicionMult) {
    this.suspicionMult = suspicionMult;
    return this;
  }

  public String namespace() {
    return namespace;
  }

  public ClusterConfig namespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  public int pingInterval() {
    return pingInterval;
  }

  public ClusterConfig pingInterval(int pingInterval) {
    this.pingInterval = pingInterval;
    return this;
  }

  public int pingReqMembers() {
    return pingReqMembers;
  }

  public ClusterConfig pingReqMembers(int pingReqMembers) {
    this.pingReqMembers = pingReqMembers;
    return this;
  }

  public int gossipFanout() {
    return gossipFanout;
  }

  public ClusterConfig gossipFanout(int gossipFanout) {
    this.gossipFanout = gossipFanout;
    return this;
  }

  public long gossipInterval() {
    return gossipInterval;
  }

  public ClusterConfig gossipInterval(long gossipInterval) {
    this.gossipInterval = gossipInterval;
    return this;
  }

  public int gossipRepeatMult() {
    return gossipRepeatMult;
  }

  public ClusterConfig gossipRepeatMult(int gossipRepeatMult) {
    this.gossipRepeatMult = gossipRepeatMult;
    return this;
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

  /**
   * A threshold for received gossip id intervals. If number of intervals is more than threshold
   * then warning will be raised, this mean that node losing network frequently for a long time.
   *
   * <p>For example if we received gossip with id 1,2 and 5 then we will have 2 intervals [1, 2],
   * [5, 5].
   *
   * @param gossipSegmentationThreshold gossipSegmentationThreshold
   * @return gossip segmentation threshold
   */
  public ClusterConfig gossipSegmentationThreshold(int gossipSegmentationThreshold) {
    this.gossipSegmentationThreshold = gossipSegmentationThreshold;
    return this;
  }

  public int payloadInterval() {
    return payloadInterval;
  }

  public ClusterConfig payloadInterval(int payloadInterval) {
    this.payloadInterval = payloadInterval;
    return this;
  }

  @Override
  public ClusterConfig clone() {
    try {
      return (ClusterConfig) super.clone();
    } catch (CloneNotSupportedException e) {
      throw Exceptions.propagate(e);
    }
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ClusterConfig.class.getSimpleName() + "[", "]")
        .add("seedMembers=" + seedMembers)
        .add("syncInterval=" + syncInterval)
        .add("suspicionMult=" + suspicionMult)
        .add("namespace='" + namespace + "'")
        .add("payloadInterval=" + payloadInterval)
        .add("pingInterval=" + pingInterval)
        .add("pingReqMembers=" + pingReqMembers)
        .add("gossipFanout=" + gossipFanout)
        .add("gossipInterval=" + gossipInterval)
        .add("gossipRepeatMult=" + gossipRepeatMult)
        .add("gossipSegmentationThreshold=" + gossipSegmentationThreshold)
        .toString();
  }
}
