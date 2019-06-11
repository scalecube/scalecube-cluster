package io.scalecube.cluster.transport.api;

import reactor.core.Exceptions;

public final class TransportConfig implements Cloneable {

  // LAN cluster
  public static final int DEFAULT_CONNECT_TIMEOUT = 3_000;

  // WAN cluster (overrides default/LAN settings)
  public static final int DEFAULT_WAN_CONNECT_TIMEOUT = 10_000;

  // Local cluster working via loopback interface (overrides default/LAN settings)
  public static final int DEFAULT_LOCAL_CONNECT_TIMEOUT = 1_000;

  public static final int DEFAULT_PORT = 0;
  public static final MessageCodec DEFAULT_MESSAGE_CODEC = MessageCodec.INSTANCE;
  public static final int DEFAULT_MAX_FRAME_LENGTH = 2 * 1024 * 1024; // 2MB

  private int port;
  private int connectTimeout;
  private MessageCodec messageCodec;
  private int maxFrameLength;

  public TransportConfig() {}

  //  private TransportConfig(TransportConfig other) {
  //    this.port = other.port;
  //    this.connectTimeout = other.connectTimeout;
  //    this.messageCodec = other.messageCodec;
  //    this.maxFrameLength = other.maxFrameLength;
  //  }

  @Override
  public TransportConfig clone() {
    try {
      return (TransportConfig) super.clone();
    } catch (CloneNotSupportedException e) {
      throw Exceptions.propagate(e);
    }
  }

  public static TransportConfig defaultConfig() {
    return new TransportConfig();
  }

  public int port() {
    return port;
  }

  public int connectTimeout() {
    return connectTimeout;
  }

  public MessageCodec messageCodec() {
    return messageCodec;
  }

  public int maxFrameLength() {
    return maxFrameLength;
  }

  @Override
  public String toString() {
    return "TransportConfig{"
        + "port="
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
