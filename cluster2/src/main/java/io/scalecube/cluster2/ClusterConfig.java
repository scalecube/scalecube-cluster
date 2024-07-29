package io.scalecube.cluster2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import org.agrona.MutableDirectBuffer;
import reactor.core.Exceptions;

public class ClusterConfig implements Cloneable {

  private List<String> seedMembers = Collections.emptyList();
  private int syncInterval = 3000;
  private int suspicionMult = 5;
  private String namespace = "default";
  private int pingInterval = 1000;
  private int pingReqMembers = 3;
  private int gossipFanout = 3;
  private long gossipInterval = 200;
  private int gossipRepeatMult = 3;
  private int gossipSegmentationThreshold = 1000;
  private MutableDirectBuffer payload;
  private int payloadLength;

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

  public MutableDirectBuffer payload() {
    return payload;
  }

  public ClusterConfig payload(MutableDirectBuffer payload) {
    this.payload = payload;
    return this;
  }

  public int payloadLength() {
    return payloadLength;
  }

  public ClusterConfig payloadLength(int payloadLength) {
    this.payloadLength = payloadLength;
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
        .add("pingInterval=" + pingInterval)
        .add("pingReqMembers=" + pingReqMembers)
        .add("gossipFanout=" + gossipFanout)
        .add("gossipInterval=" + gossipInterval)
        .add("gossipRepeatMult=" + gossipRepeatMult)
        .add("gossipSegmentationThreshold=" + gossipSegmentationThreshold)
        .add("payload=" + payload)
        .add("payloadLength=" + payloadLength)
        .toString();
  }
}
