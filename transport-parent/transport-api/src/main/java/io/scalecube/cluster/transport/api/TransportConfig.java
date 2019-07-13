package io.scalecube.cluster.transport.api;

import reactor.core.Exceptions;

public final class TransportConfig implements Cloneable {

  // LAN cluster
  public static final int DEFAULT_CONNECT_TIMEOUT = 3_000;

  // WAN cluster (overrides default/LAN settings)
  public static final int DEFAULT_WAN_CONNECT_TIMEOUT = 10_000;

  // Local cluster working via loopback interface (overrides default/LAN settings)
  public static final int DEFAULT_LOCAL_CONNECT_TIMEOUT = 1_000;

  private String host = null;
  private int port = 0;
  private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
  private MessageCodec messageCodec = MessageCodec.INSTANCE;
  private int maxFrameLength = 2 * 1024 * 1024; // 2 MB

  public TransportConfig() {}

  public static TransportConfig defaultConfig() {
    return new TransportConfig();
  }

  /**
   * Creates {@code ClusterConfig} with default settings for cluster on LAN network.
   *
   * @return new {@code ClusterConfig}
   */
  public static TransportConfig defaultLanConfig() {
    return defaultConfig();
  }

  /**
   * Creates {@code ClusterConfig} with default settings for cluster on WAN network.
   *
   * @return new {@code ClusterConfig}
   */
  public static TransportConfig defaultWanConfig() {
    return defaultConfig().connectTimeout(DEFAULT_WAN_CONNECT_TIMEOUT);
  }

  /**
   * Creates {@code MembershipConfig} with default settings for cluster on local loopback interface.
   *
   * @return new {@code MembershipConfig}
   */
  public static TransportConfig defaultLocalConfig() {
    return defaultConfig().connectTimeout(DEFAULT_LOCAL_CONNECT_TIMEOUT).host("localhost");
  }

  public String host() {
    return host;
  }

  /**
   * Sets a host.
   *
   * @param host host
   * @return new {@code TransportConfig} instance
   */
  public TransportConfig host(String host) {
    TransportConfig t = clone();
    t.host = host;
    return t;
  }

  public int port() {
    return port;
  }

  /**
   * Sets a port.
   *
   * @param port port
   * @return new {@code TransportConfig} instance
   */
  public TransportConfig port(int port) {
    TransportConfig t = clone();
    t.port = port;
    return t;
  }

  public int connectTimeout() {
    return connectTimeout;
  }

  /**
   * Sets a connectTimeout.
   *
   * @param connectTimeout connect timeout
   * @return new {@code TransportConfig} instance
   */
  public TransportConfig connectTimeout(int connectTimeout) {
    TransportConfig t = clone();
    t.connectTimeout = connectTimeout;
    return t;
  }

  public MessageCodec messageCodec() {
    return messageCodec;
  }

  /**
   * Sets a messageCodec.
   *
   * @param messageCodec message codec
   * @return new {@code TransportConfig} instance
   */
  public TransportConfig messageCodec(MessageCodec messageCodec) {
    TransportConfig t = clone();
    t.messageCodec = messageCodec;
    return t;
  }

  public int maxFrameLength() {
    return maxFrameLength;
  }

  /**
   * Sets a maxFrameLength.
   *
   * @param maxFrameLength max frame length
   * @return new {@code TransportConfig} instance
   */
  public TransportConfig maxFrameLength(int maxFrameLength) {
    TransportConfig t = clone();
    t.maxFrameLength = maxFrameLength;
    return t;
  }

  @Override
  public TransportConfig clone() {
    try {
      return (TransportConfig) super.clone();
    } catch (CloneNotSupportedException e) {
      throw Exceptions.propagate(e);
    }
  }

  @Override
  public String toString() {
    return "TransportConfig{"
        + "host="
        + host
        + ", port="
        + port
        + ", connectTimeout="
        + connectTimeout
        + ", messageCodec="
        + messageCodec
        + ", maxFrameLength="
        + maxFrameLength
        + '}';
  }
}
