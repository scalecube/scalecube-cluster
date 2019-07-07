package io.scalecube.cluster;

import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.cluster.metadata.MetadataDecoder;
import io.scalecube.cluster.metadata.MetadataEncoder;
import io.scalecube.cluster.transport.api.TransportConfig;
import java.util.Optional;
import java.util.function.UnaryOperator;
import reactor.core.Exceptions;

/**
 * Cluster configuration encapsulate settings needed cluster to create and successfully join.
 *
 * @see MembershipConfig
 * @see FailureDetectorConfig
 * @see GossipConfig
 * @see TransportConfig
 */
public final class ClusterConfig implements Cloneable {

  // LAN cluster
  public static final int DEFAULT_METADATA_TIMEOUT = 3_000;

  // WAN cluster (overrides default/LAN settings)
  public static final int DEFAULT_WAN_METADATA_TIMEOUT = 10_000;

  // Local cluster working via loopback interface (overrides default/LAN settings)
  public static final int DEFAULT_LOCAL_METADATA_TIMEOUT = 1_000;

  private Object metadata;
  private int metadataTimeout = DEFAULT_METADATA_TIMEOUT;
  private MetadataEncoder metadataEncoder = MetadataEncoder.INSTANCE;
  private MetadataDecoder metadataDecoder = MetadataDecoder.INSTANCE;

  private String memberHost;
  private Integer memberPort;

  private TransportConfig transportConfig = TransportConfig.defaultConfig();
  private FailureDetectorConfig failureDetectorConfig = FailureDetectorConfig.defaultConfig();
  private GossipConfig gossipConfig = GossipConfig.defaultConfig();
  private MembershipConfig membershipConfig = MembershipConfig.defaultConfig();

  public ClusterConfig() {}

  public static ClusterConfig defaultConfig() {
    return new ClusterConfig();
  }

  /**
   * Creates {@code ClusterConfig} with default settings for cluster on LAN network.
   *
   * @return new {@code ClusterConfig}
   */
  public static ClusterConfig defaultLanConfig() {
    return defaultConfig();
  }

  /**
   * Creates {@code ClusterConfig} with default settings for cluster on WAN network.
   *
   * @return new {@code ClusterConfig}
   */
  public static ClusterConfig defaultWanConfig() {
    return defaultConfig()
        .membership(
            opts ->
                opts.suspicionMult(MembershipConfig.DEFAULT_WAN_SUSPICION_MULT)
                    .syncInterval(MembershipConfig.DEFAULT_WAN_SYNC_INTERVAL))
        .transport(opts -> opts.connectTimeout(TransportConfig.DEFAULT_WAN_CONNECT_TIMEOUT))
        .metadataTimeout(DEFAULT_WAN_METADATA_TIMEOUT);
  }

  /**
   * Creates {@code MembershipConfig} with default settings for cluster on local loopback interface.
   *
   * @return new {@code MembershipConfig}
   */
  public static ClusterConfig defaultLocalConfig() {
    return defaultConfig()
        .membership(
            opts ->
                opts.suspicionMult(MembershipConfig.DEFAULT_LOCAL_SUSPICION_MULT)
                    .syncInterval(MembershipConfig.DEFAULT_LOCAL_SYNC_INTERVAL))
        .transport(opts -> opts.connectTimeout(TransportConfig.DEFAULT_LOCAL_CONNECT_TIMEOUT))
        .metadataTimeout(DEFAULT_LOCAL_METADATA_TIMEOUT);
  }

  public <T> T metadata() {
    //noinspection unchecked
    return (T) metadata;
  }

  /**
   * Sets a metadata.
   *
   * @param metadata metadata
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig metadata(Object metadata) {
    ClusterConfig c = clone();
    c.metadata = metadata;
    return c;
  }

  public int metadataTimeout() {
    return metadataTimeout;
  }

  /**
   * Sets a metadataTimeout.
   *
   * @param metadataTimeout metadata timeout
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig metadataTimeout(int metadataTimeout) {
    ClusterConfig c = clone();
    c.metadataTimeout = metadataTimeout;
    return c;
  }

  public MetadataEncoder metadataEncoder() {
    return metadataEncoder;
  }

  /**
   * Sets a metadataEncoder.
   *
   * @param metadataEncoder metadata encoder
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig metadataEncoder(MetadataEncoder metadataEncoder) {
    ClusterConfig c = clone();
    c.metadataEncoder = metadataEncoder;
    return c;
  }

  public MetadataDecoder metadataDecoder() {
    return metadataDecoder;
  }

  /**
   * Sets a metadataDecoder.
   *
   * @param metadataDecoder metadata decoder
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig metadataDecoder(MetadataDecoder metadataDecoder) {
    ClusterConfig c = clone();
    c.metadataDecoder = metadataDecoder;
    return c;
  }

  public String memberHost() {
    return memberHost;
  }

  /**
   * Sets a memberHost.
   *
   * @param memberHost member host
   * @return new {@code ClusterConfig} instance@return
   */
  public ClusterConfig memberHost(String memberHost) {
    ClusterConfig c = clone();
    c.memberHost = memberHost;
    return c;
  }

  public Integer memberPort() {
    return memberPort;
  }

  /**
   * Sets a memberPort.
   *
   * @param memberPort member port
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig memberPort(Integer memberPort) {
    ClusterConfig c = clone();
    c.memberPort = memberPort;
    return c;
  }

  /**
   * Applies {@link TransportConfig} settings.
   *
   * @param op operator to apply {@link TransportConfig} settings
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig transport(UnaryOperator<TransportConfig> op) {
    ClusterConfig c = clone();
    c.transportConfig = op.apply(transportConfig);
    return c;
  }

  public TransportConfig transportConfig() {
    return transportConfig;
  }

  /**
   * Applies {@link FailureDetectorConfig} settings.
   *
   * @param op operator to apply {@link FailureDetectorConfig} settings
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig failureDetector(UnaryOperator<FailureDetectorConfig> op) {
    ClusterConfig c = clone();
    c.failureDetectorConfig = op.apply(failureDetectorConfig);
    return c;
  }

  public FailureDetectorConfig failureDetectorConfig() {
    return failureDetectorConfig;
  }

  /**
   * Applies {@link GossipConfig} settings.
   *
   * @param op operator to apply {@link GossipConfig} settings
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig gossip(UnaryOperator<GossipConfig> op) {
    ClusterConfig c = clone();
    c.gossipConfig = op.apply(gossipConfig);
    return c;
  }

  public GossipConfig gossipConfig() {
    return gossipConfig;
  }

  /**
   * Applies {@link MembershipConfig} settings.
   *
   * @param op operator to apply {@link MembershipConfig} settings
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig membership(UnaryOperator<MembershipConfig> op) {
    ClusterConfig c = clone();
    c.membershipConfig = op.apply(membershipConfig);
    return c;
  }

  public MembershipConfig membershipConfig() {
    return membershipConfig;
  }

  @Override
  public ClusterConfig clone() {
    try {
      ClusterConfig c = (ClusterConfig) super.clone();
      c.transportConfig = transportConfig.clone();
      c.failureDetectorConfig = failureDetectorConfig.clone();
      c.gossipConfig = gossipConfig.clone();
      c.membershipConfig = membershipConfig.clone();
      return c;
    } catch (CloneNotSupportedException e) {
      throw Exceptions.propagate(e);
    }
  }

  @Override
  public String toString() {
    return "ClusterConfig{"
        + "metadata="
        + metadataAsString()
        + ", metadataTimeout="
        + metadataTimeout
        + ", metadataEncoder="
        + metadataEncoder
        + ", metadataDecoder="
        + metadataDecoder
        + ", memberHost='"
        + memberHost
        + '\''
        + ", memberPort="
        + memberPort
        + ", transportConfig="
        + transportConfig
        + ", failureDetectorConfig="
        + failureDetectorConfig
        + ", gossipConfig="
        + gossipConfig
        + ", membershipConfig="
        + membershipConfig
        + '}';
  }

  private String metadataAsString() {
    return Optional.ofNullable(metadata)
        .map(Object::hashCode)
        .map(Integer::toHexString)
        .orElse(null);
  }
}
