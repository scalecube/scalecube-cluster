package io.scalecube.cluster.transport;

/**
 * Encapsulate transport settings.
 * 
 */
public final class TransportConfig {

  public static final String DEFAULT_LISTEN_ADDRESS = null;
  public static final String DEFAULT_LISTEN_INTERFACE = null; // Default listen settings fallback to getLocalHost
  public static final boolean DEFAULT_PREFER_IP6 = false;
  public static final int DEFAULT_PORT = 0;
  public static final int DEFAULT_CONNECT_TIMEOUT = 3000;
  public static final boolean DEFAULT_USE_NETWORK_EMULATOR = false;
  public static final boolean DEFAULT_ENABLE_EPOLL = true;
  public static final int DEFAULT_BOSS_THREADS = 2;
  public static final int DEFAULT_WORKER_THREADS = 0;

  private final String listenAddress;
  private final String listenInterface;
  private final boolean preferIPv6;
  private final int port;
  private final int connectTimeout;
  private final boolean useNetworkEmulator;
  private final boolean enableEpoll;
  private final int bossThreads;
  private final int workerThreads;

  private TransportConfig(Builder builder) {
    this.listenAddress = builder.listenAddress;
    this.listenInterface = builder.listenInterface;
    this.preferIPv6 = builder.preferIPv6;
    this.port = builder.port;
    this.connectTimeout = builder.connectTimeout;
    this.useNetworkEmulator = builder.useNetworkEmulator;
    this.enableEpoll = builder.enableEpoll;
    this.bossThreads = builder.bossThreads;
    this.workerThreads = builder.workerThreads;
  }

  public static TransportConfig defaultConfig() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getListenAddress() {
    return listenAddress;
  }

  public String getListenInterface() {
    return listenInterface;
  }

  public boolean isPreferIPv6() {
    return preferIPv6;
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

  public boolean isEnableEpoll() {
    return enableEpoll;
  }

  public int getBossThreads() {
    return bossThreads;
  }

  public int getWorkerThreads() {
    return workerThreads;
  }

  @Override
  public String toString() {
    return "TransportConfig{listenAddress=" + listenAddress
        + ", listenInterface=" + listenInterface
        + ", preferIPv6=" + preferIPv6
        + ", port=" + port
        + ", connectTimeout=" + connectTimeout
        + ", useNetworkEmulator=" + useNetworkEmulator
        + ", enableEpoll=" + enableEpoll
        + ", bossThreads=" + bossThreads
        + ", workerThreads=" + workerThreads
        + '}';
  }

  public static final class Builder {

    private String listenAddress = DEFAULT_LISTEN_ADDRESS;
    private String listenInterface = DEFAULT_LISTEN_INTERFACE;
    private boolean preferIPv6 = DEFAULT_PREFER_IP6;
    private int port = DEFAULT_PORT;
    private boolean useNetworkEmulator = DEFAULT_USE_NETWORK_EMULATOR;
    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private boolean enableEpoll = DEFAULT_ENABLE_EPOLL;
    private int bossThreads = DEFAULT_BOSS_THREADS;
    private int workerThreads = DEFAULT_WORKER_THREADS;

    private Builder() {}

    /**
     * Fills config with values equal to provided config.
     *
     * @param config trasport config
     */
    public Builder fillFrom(TransportConfig config) {
      this.listenAddress = config.listenAddress;
      this.listenInterface = config.listenInterface;
      this.preferIPv6 = config.preferIPv6;
      this.port = config.port;
      this.connectTimeout = config.connectTimeout;
      this.useNetworkEmulator = config.useNetworkEmulator;
      this.enableEpoll = config.enableEpoll;
      this.bossThreads = config.bossThreads;
      this.workerThreads = config.workerThreads;
      return this;
    }

    public Builder listenAddress(String listenAddress) {
      this.listenAddress = listenAddress;
      return this;
    }

    public Builder listenInterface(String listenInterface) {
      this.listenInterface = listenInterface;
      return this;
    }

    public Builder preferIPv6(boolean preferIPv6) {
      this.preferIPv6 = preferIPv6;
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

    public Builder enableEpoll(boolean enableEpoll) {
      this.enableEpoll = enableEpoll;
      return this;
    }

    public Builder bossThreads(int bossThreads) {
      this.bossThreads = bossThreads;
      return this;
    }

    public Builder workerThreads(int workerThreads) {
      this.workerThreads = workerThreads;
      return this;
    }

    public TransportConfig build() {
      return new TransportConfig(this);
    }
  }
}
