package io.scalecube.cluster;

import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.cluster.metadata.MetadataCodec;
import io.scalecube.cluster.transport.api.TransportConfig;
import java.util.Optional;
import java.util.StringJoiner;
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
  private MetadataCodec metadataCodec = MetadataCodec.INSTANCE;

  private String memberId;
  private String memberAlias;
  private String externalHost;
  private Integer externalPort;

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
        .failureDetector(opts -> FailureDetectorConfig.defaultWanConfig())
        .gossip(opts -> GossipConfig.defaultWanConfig())
        .membership(opts -> MembershipConfig.defaultWanConfig())
        .transport(opts -> TransportConfig.defaultWanConfig())
        .metadataTimeout(DEFAULT_WAN_METADATA_TIMEOUT);
  }

  /**
   * Creates {@code MembershipConfig} with default settings for cluster on local loopback interface.
   *
   * @return new {@code MembershipConfig}
   */
  public static ClusterConfig defaultLocalConfig() {
    return defaultConfig()
        .failureDetector(opts -> FailureDetectorConfig.defaultLocalConfig())
        .gossip(opts -> GossipConfig.defaultLocalConfig())
        .membership(opts -> MembershipConfig.defaultLocalConfig())
        .transport(opts -> TransportConfig.defaultLocalConfig())
        .metadataTimeout(DEFAULT_LOCAL_METADATA_TIMEOUT);
  }

  public <T> T metadata() {
    //noinspection unchecked
    return (T) metadata;
  }

  /**
   * Setter for metadata.
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
   * Setter for metadataTimeout.
   *
   * @param metadataTimeout metadata timeout
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig metadataTimeout(int metadataTimeout) {
    ClusterConfig c = clone();
    c.metadataTimeout = metadataTimeout;
    return c;
  }

  public MetadataCodec metadataCodec() {
    return metadataCodec;
  }

  /**
   * Setter for metadataCodec.
   *
   * @param metadataCodec metadata codec
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig metadataCodec(MetadataCodec metadataCodec) {
    ClusterConfig c = clone();
    c.metadataCodec = metadataCodec;
    return c;
  }

  /**
   * Returns externalHost. {@code externalHost} is a config property for container environments,
   * it's being set for advertising to scalecube cluster some connectable hostname which maps to
   * scalecube transport's hostname on which scalecube transport is listening.
   *
   * @return external host
   */
  public String externalHost() {
    return externalHost;
  }

  /**
   * Setter for externalHost. {@code externalHost} is a config property for container environments,
   * it's being set for advertising to scalecube cluster some connectable hostname which maps to
   * scalecube transport's hostname on which scalecube transport is listening.
   *
   * @param externalHost external host
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig externalHost(String externalHost) {
    ClusterConfig c = clone();
    c.externalHost = externalHost;
    return c;
  }

  /**
   * Returns ID to use for the local member. If {@code null}, the ID will be generated
   * automatically.
   *
   * @return local member ID.
   */
  public String memberId() {
    return memberId;
  }

  /**
   * Sets ID to use for the local member. If {@code null}, the ID will be generated automatically.
   *
   * @param memberId local member ID
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig memberId(String memberId) {
    ClusterConfig c = clone();
    c.memberId = memberId;
    return c;
  }

  /**
   * Returns memberAlias. {@code memberAlias} is a config property which facilitates {@link
   * io.scalecube.cluster.Member#toString()}.
   *
   * @return member alias.
   */
  public String memberAlias() {
    return memberAlias;
  }

  /**
   * Setter for memberAlias. {@code memberAlias} is a config property which facilitates {@link
   * io.scalecube.cluster.Member#toString()}.
   *
   * @param memberAlias member alias
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig memberAlias(String memberAlias) {
    ClusterConfig c = clone();
    c.memberAlias = memberAlias;
    return c;
  }

  /**
   * Returns externalPort. {@code externalPort} is a config property for container environments,
   * it's being set for advertising to scalecube cluster a port which mapped to scalecube
   * transport's listening port.
   *
   * @return external port
   */
  public Integer externalPort() {
    return externalPort;
  }

  /**
   * Setter for externalPort. {@code externalPort} is a config property for container environments,
   * it's being set for advertising to scalecube cluster a port which mapped to scalecube
   * transport's listening port.
   *
   * @param externalPort external port
   * @return new {@code ClusterConfig} instance
   */
  public ClusterConfig externalPort(Integer externalPort) {
    ClusterConfig c = clone();
    c.externalPort = externalPort;
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
    return new StringJoiner(", ", ClusterConfig.class.getSimpleName() + "[", "]")
        .add("metadata=" + metadataAsString())
        .add("metadataTimeout=" + metadataTimeout)
        .add("metadataCodec=" + metadataCodec)
        .add("memberId='" + memberId + "'")
        .add("memberAlias='" + memberAlias + "'")
        .add("externalHost='" + externalHost + "'")
        .add("externalPort=" + externalPort)
        .add("transportConfig=" + transportConfig)
        .add("failureDetectorConfig=" + failureDetectorConfig)
        .add("gossipConfig=" + gossipConfig)
        .add("membershipConfig=" + membershipConfig)
        .toString();
  }

  private String metadataAsString() {
    return Optional.ofNullable(metadata)
        .map(Object::hashCode)
        .map(Integer::toHexString)
        .orElse(null);
  }
}
