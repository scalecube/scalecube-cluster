package io.scalecube.cluster;

import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.cluster.metadata.MetadataDecoder;
import io.scalecube.cluster.metadata.MetadataEncoder;
import io.scalecube.cluster.transport.api.TransportConfig;
import java.util.Optional;

/**
 * Cluster configuration encapsulate settings needed cluster to create and successfully join.
 *
 * @see MembershipConfig
 * @see FailureDetectorConfig
 * @see GossipConfig
 * @see TransportConfig
 */
public final class ClusterConfig {

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
        .suspicionMult(DEFAULT_WAN_SUSPICION_MULT)
        .syncInterval(DEFAULT_WAN_SYNC_INTERVAL)
        .connectTimeout(DEFAULT_WAN_CONNECT_TIMEOUT)
        .metadataTimeout(DEFAULT_WAN_METADATA_TIMEOUT)
        .build();
  }

  /**
   * Creates {@code MembershipConfig} with default settings for cluster on local loopback interface.
   *
   * @return new {@code MembershipConfig}
   */
  public static ClusterConfig defaultLocalConfig() {
    return defaultConfig()
        .suspicionMult(DEFAULT_LOCAL_SUSPICION_MULT)
        .syncInterval(DEFAULT_LOCAL_SYNC_INTERVAL)
        .connectTimeout(DEFAULT_LOCAL_CONNECT_TIMEOUT)
        .connectTimeout(DEFAULT_LOCAL_METADATA_TIMEOUT)
        .build();
  }

  public Object metadata() {
    return metadata;
  }

  public int metadataTimeout() {
    return metadataTimeout;
  }

  public MetadataEncoder metadataEncoder() {
    return metadataEncoder;
  }

  public MetadataDecoder metadataDecoder() {
    return metadataDecoder;
  }

  public String memberHost() {
    return memberHost;
  }

  public Integer memberPort() {
    return memberPort;
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
