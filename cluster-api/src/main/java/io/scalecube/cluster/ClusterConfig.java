package io.scalecube.cluster;

import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.cluster.metadata.MetadataCodec;
import io.scalecube.cluster.transport.api.TransportConfig;
import java.util.Optional;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.function.UnaryOperator;

/**
 * Cluster configuration. Every scalar setting is read from {@link Properties} (by default {@link
 * System#getProperties()}), keys are {@code scalecube.cluster.*}. A property set to {@code @null}
 * is treated as not set. Setters mutate this instance and return it.
 *
 * @see MembershipConfig
 * @see FailureDetectorConfig
 * @see GossipConfig
 * @see TransportConfig
 */
public final class ClusterConfig {

  public static final int DEFAULT_METADATA_TIMEOUT = 3_000;

  public static final String METADATA_TIMEOUT_PROP_NAME = "scalecube.cluster.metadataTimeout";
  public static final String MEMBER_ID_PROP_NAME = "scalecube.cluster.memberId";
  public static final String MEMBER_ALIAS_PROP_NAME = "scalecube.cluster.memberAlias";
  public static final String EXTERNAL_HOST_PROP_NAME = "scalecube.cluster.externalHost";
  public static final String EXTERNAL_PORT_PROP_NAME = "scalecube.cluster.externalPort";
  public static final String METADATA_CODEC_PROP_NAME = "scalecube.cluster.metadataCodec";

  private Object metadata;
  private int metadataTimeout;
  private MetadataCodec metadataCodec;

  private String memberId;
  private String memberAlias;
  private String externalHost;
  private Integer externalPort;

  private TransportConfig transportConfig;
  private FailureDetectorConfig failureDetectorConfig;
  private GossipConfig gossipConfig;
  private MembershipConfig membershipConfig;

  public ClusterConfig() {
    this(System.getProperties());
  }

  public ClusterConfig(Properties properties) {
    metadataTimeout(properties);
    metadataCodec(properties);
    memberId(properties);
    memberAlias(properties);
    externalHost(properties);
    externalPort(properties);
    transportConfig = new TransportConfig(properties);
    failureDetectorConfig = new FailureDetectorConfig(properties);
    gossipConfig = new GossipConfig(properties);
    membershipConfig = new MembershipConfig(properties);
  }

  private static String getProperty(Properties properties, String name) {
    final var value = properties.getProperty(name);
    return "@null".equals(value) ? null : value;
  }

  private static int getProperty(Properties properties, String name, int defaultValue) {
    final var value = getProperty(properties, name);
    return value != null ? Integer.parseInt(value) : defaultValue;
  }

  // Same as Aeron's suppliers: the property is a class name with a public no-arg constructor
  private static <T> T newInstance(String name, String className, Class<T> type) {
    try {
      return Class.forName(className).asSubclass(type).getConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException(name + ": cannot instantiate " + className, e);
    }
  }

  public <T> T metadata() {
    //noinspection unchecked
    return (T) metadata;
  }

  public ClusterConfig metadata(Object metadata) {
    this.metadata = metadata;
    return this;
  }

  public int metadataTimeout() {
    return metadataTimeout;
  }

  public ClusterConfig metadataTimeout(int metadataTimeout) {
    this.metadataTimeout = metadataTimeout;
    return this;
  }

  public ClusterConfig metadataTimeout(Properties properties) {
    return metadataTimeout(
        getProperty(properties, METADATA_TIMEOUT_PROP_NAME, DEFAULT_METADATA_TIMEOUT));
  }

  public MetadataCodec metadataCodec() {
    return metadataCodec;
  }

  public ClusterConfig metadataCodec(MetadataCodec metadataCodec) {
    this.metadataCodec = metadataCodec;
    return this;
  }

  /**
   * Reads the {@link MetadataCodec} class name; when not set, the first {@code ServiceLoader}
   * provider (or JDK serialization) is used, see {@link MetadataCodec#INSTANCE}.
   *
   * @param properties properties
   * @return this
   */
  public ClusterConfig metadataCodec(Properties properties) {
    final var className = getProperty(properties, METADATA_CODEC_PROP_NAME);
    return metadataCodec(
        className != null
            ? newInstance(METADATA_CODEC_PROP_NAME, className, MetadataCodec.class)
            : MetadataCodec.INSTANCE);
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

  public ClusterConfig memberId(String memberId) {
    this.memberId = memberId;
    return this;
  }

  public ClusterConfig memberId(Properties properties) {
    return memberId(getProperty(properties, MEMBER_ID_PROP_NAME));
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

  public ClusterConfig memberAlias(String memberAlias) {
    this.memberAlias = memberAlias;
    return this;
  }

  public ClusterConfig memberAlias(Properties properties) {
    return memberAlias(getProperty(properties, MEMBER_ALIAS_PROP_NAME));
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

  public ClusterConfig externalHost(String externalHost) {
    this.externalHost = externalHost;
    return this;
  }

  public ClusterConfig externalHost(Properties properties) {
    return externalHost(getProperty(properties, EXTERNAL_HOST_PROP_NAME));
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

  public ClusterConfig externalPort(Integer externalPort) {
    this.externalPort = externalPort;
    return this;
  }

  public ClusterConfig externalPort(Properties properties) {
    final var value = getProperty(properties, EXTERNAL_PORT_PROP_NAME);
    return externalPort(value != null ? Integer.valueOf(value) : null);
  }

  public TransportConfig transportConfig() {
    return transportConfig;
  }

  public ClusterConfig transport(UnaryOperator<TransportConfig> op) {
    transportConfig = op.apply(transportConfig);
    return this;
  }

  public FailureDetectorConfig failureDetectorConfig() {
    return failureDetectorConfig;
  }

  public ClusterConfig failureDetector(UnaryOperator<FailureDetectorConfig> op) {
    failureDetectorConfig = op.apply(failureDetectorConfig);
    return this;
  }

  public GossipConfig gossipConfig() {
    return gossipConfig;
  }

  public ClusterConfig gossip(UnaryOperator<GossipConfig> op) {
    gossipConfig = op.apply(gossipConfig);
    return this;
  }

  public MembershipConfig membershipConfig() {
    return membershipConfig;
  }

  public ClusterConfig membership(UnaryOperator<MembershipConfig> op) {
    membershipConfig = op.apply(membershipConfig);
    return this;
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
