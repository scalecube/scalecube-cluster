package io.scalecube.cluster.transport.api;

import java.util.StringJoiner;
import java.util.function.Function;
import reactor.core.Exceptions;

public final class TransportConfig implements Cloneable {

  // LAN cluster
  public static final int DEFAULT_CONNECT_TIMEOUT = 3_000;

  // WAN cluster (overrides default/LAN settings)
  public static final int DEFAULT_WAN_CONNECT_TIMEOUT = 10_000;

  // Local cluster working via loopback interface (overrides default/LAN settings)
  public static final int DEFAULT_LOCAL_CONNECT_TIMEOUT = 1_000;

  private int port = 0;
  private boolean clientSecured = false; // client secured flag
  private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
  private MessageCodec messageCodec = MessageCodec.INSTANCE;
  private int maxFrameLength = 2 * 1024 * 1024; // 2 MB
  private TransportFactory transportFactory;
  private Function<String, String> addressMapper = Function.identity();

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
    return defaultConfig().connectTimeout(DEFAULT_LOCAL_CONNECT_TIMEOUT);
  }

  public int port() {
    return port;
  }

  /**
   * Setter for {@code port}.
   *
   * @param port port
   * @return new {@code TransportConfig} instance
   */
  public TransportConfig port(int port) {
    TransportConfig t = clone();
    t.port = port;
    return t;
  }

  public boolean isClientSecured() {
    return clientSecured;
  }

  /**
   * Setter to denote whether client part of the transport is secured.
   *
   * @param clientSecured clientSecured
   * @return new {@code TransportConfig} instance
   */
  public TransportConfig clientSecured(boolean clientSecured) {
    TransportConfig t = clone();
    t.clientSecured = clientSecured;
    return t;
  }

  public int connectTimeout() {
    return connectTimeout;
  }

  /**
   * Setter for {@code connectTimeout}.
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
   * Setter for {@code messageCodec}.
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
   * Setter for {@code maxFrameLength}.
   *
   * @param maxFrameLength max frame length
   * @return new {@code TransportConfig} instance
   */
  public TransportConfig maxFrameLength(int maxFrameLength) {
    TransportConfig t = clone();
    t.maxFrameLength = maxFrameLength;
    return t;
  }

  /**
   * Setter for {@code addressMapper}.
   *
   * @param addressMapper address mapper
   * @return new {@code TransportConfig} instance
   */
  public TransportConfig addressMapper(Function<String, String> addressMapper) {
    TransportConfig t = clone();
    t.addressMapper = addressMapper;
    return t;
  }

  public Function<String, String> addressMapper() {
    return addressMapper;
  }

  public TransportFactory transportFactory() {
    return transportFactory;
  }

  /**
   * Setter for {@code transportFactory}.
   *
   * @param transportFactory transport factory
   * @return new {@code TransportConfig} instance
   */
  public TransportConfig transportFactory(TransportFactory transportFactory) {
    TransportConfig t = clone();
    t.transportFactory = transportFactory;
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
    return new StringJoiner(", ", TransportConfig.class.getSimpleName() + "[", "]")
        .add("port=" + port)
        .add("clientSecured=" + clientSecured)
        .add("connectTimeout=" + connectTimeout)
        .add("messageCodec=" + messageCodec)
        .add("maxFrameLength=" + maxFrameLength)
        .add("transportFactory=" + transportFactory)
        .add("addressMapper=" + addressMapper)
        .toString();
  }
}
