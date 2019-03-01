package io.scalecube.transport;

public final class TransportConfig {

  public static final int DEFAULT_PORT = 0;
  public static final int DEFAULT_CONNECT_TIMEOUT = 3000;
  public static final boolean DEFAULT_USE_NETWORK_EMULATOR = false;
  public static final MessageCodec DEFAULT_MESSAGE_CODEC = new JacksonMessageCodec();
  public static final int DEFAULT_MAX_FRAME_LENGTH = 2 * 1024 * 1024; // 2MB

  private final int port;
  private final int connectTimeout;
  private final boolean useNetworkEmulator;
  private final MessageCodec messageCodec;
  private final int maxFrameLength;

  private TransportConfig(Builder builder) {
    this.port = builder.port;
    this.connectTimeout = builder.connectTimeout;
    this.useNetworkEmulator = builder.useNetworkEmulator;
    this.messageCodec = builder.messageCodec;
    this.maxFrameLength = builder.maxFrameLength;
  }

  public static TransportConfig defaultConfig() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getPort() {
    return port;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public boolean isUseNetworkEmulator() {
    return useNetworkEmulator;
  }

  public MessageCodec getMessageCodec() {
    return messageCodec;
  }

  public int getMaxFrameLength() {
    return maxFrameLength;
  }

  @Override
  public String toString() {
    return "TransportConfig{port="
        + port
        + ", connectTimeout="
        + connectTimeout
        + ", useNetworkEmulator="
        + useNetworkEmulator
        + ", messageCodec="
        + messageCodec
        + ", maxFrameLength="
        + maxFrameLength
        + '}';
  }

  public static final class Builder {

    private int port = DEFAULT_PORT;
    private boolean useNetworkEmulator = DEFAULT_USE_NETWORK_EMULATOR;
    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private MessageCodec messageCodec = DEFAULT_MESSAGE_CODEC;
    private int maxFrameLength = DEFAULT_MAX_FRAME_LENGTH;

    private Builder() {}

    /**
     * Fills config with values equal to provided config.
     *
     * @param config transport config
     */
    public Builder fillFrom(TransportConfig config) {
      this.port = config.port;
      this.connectTimeout = config.connectTimeout;
      this.useNetworkEmulator = config.useNetworkEmulator;
      this.messageCodec = config.messageCodec;
      this.maxFrameLength = config.maxFrameLength;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder connectTimeout(int connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder useNetworkEmulator(boolean useNetworkEmulator) {
      this.useNetworkEmulator = useNetworkEmulator;
      return this;
    }

    public Builder messageCodec(MessageCodec messageCodec) {
      this.messageCodec = messageCodec;
      return this;
    }

    public Builder maxFrameLength(int maxFrameLength) {
      this.maxFrameLength = maxFrameLength;
      return this;
    }

    /**
     * Finish configuration.
     *
     * @return transport config
     */
    public TransportConfig build() {
      return new TransportConfig(this);
    }
  }
}
