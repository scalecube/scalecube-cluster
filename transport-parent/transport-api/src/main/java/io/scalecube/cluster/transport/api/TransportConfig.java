package io.scalecube.cluster.transport.api;

import java.util.Properties;
import java.util.StringJoiner;
import java.util.function.Function;

public final class TransportConfig {

  public static final int DEFAULT_PORT = 0;
  public static final boolean DEFAULT_CLIENT_SECURED = false;
  public static final int DEFAULT_CONNECT_TIMEOUT = 3_000;
  public static final int DEFAULT_MAX_FRAME_LENGTH = 2 * 1024 * 1024;

  public static final String PORT_PROP_NAME = "scalecube.cluster.transport.port";
  public static final String CLIENT_SECURED_PROP_NAME = "scalecube.cluster.transport.clientSecured";
  public static final String CONNECT_TIMEOUT_PROP_NAME =
      "scalecube.cluster.transport.connectTimeout";
  public static final String MAX_FRAME_LENGTH_PROP_NAME =
      "scalecube.cluster.transport.maxFrameLength";
  public static final String MESSAGE_CODEC_PROP_NAME = "scalecube.cluster.transport.messageCodec";
  public static final String TRANSPORT_FACTORY_PROP_NAME =
      "scalecube.cluster.transport.transportFactory";

  private int port;
  private boolean clientSecured;
  private int connectTimeout;
  private int maxFrameLength;
  private MessageCodec messageCodec;
  private TransportFactory transportFactory;
  private Function<String, String> addressMapper = Function.identity();

  public TransportConfig() {
    this(System.getProperties());
  }

  public TransportConfig(Properties properties) {
    port(properties);
    clientSecured(properties);
    connectTimeout(properties);
    maxFrameLength(properties);
    messageCodec(properties);
    transportFactory(properties);
  }

  private static String getProperty(Properties properties, String name) {
    final var value = properties.getProperty(name);
    return "@null".equals(value) ? null : value;
  }

  private static int getProperty(Properties properties, String name, int defaultValue) {
    final var value = getProperty(properties, name);
    return value != null ? Integer.parseInt(value) : defaultValue;
  }

  private static boolean getProperty(Properties properties, String name, boolean defaultValue) {
    final var value = getProperty(properties, name);
    return value != null ? Boolean.parseBoolean(value) : defaultValue;
  }

  // Same as Aeron's suppliers: the property is a class name with a public no-arg constructor
  private static <T> T newInstance(String name, String className, Class<T> type) {
    try {
      return Class.forName(className).asSubclass(type).getConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException(name + ": cannot instantiate " + className, e);
    }
  }

  public int port() {
    return port;
  }

  public TransportConfig port(int port) {
    this.port = port;
    return this;
  }

  public TransportConfig port(Properties properties) {
    return port(getProperty(properties, PORT_PROP_NAME, DEFAULT_PORT));
  }

  public boolean isClientSecured() {
    return clientSecured;
  }

  public TransportConfig clientSecured(boolean clientSecured) {
    this.clientSecured = clientSecured;
    return this;
  }

  public TransportConfig clientSecured(Properties properties) {
    return clientSecured(getProperty(properties, CLIENT_SECURED_PROP_NAME, DEFAULT_CLIENT_SECURED));
  }

  public int connectTimeout() {
    return connectTimeout;
  }

  public TransportConfig connectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  public TransportConfig connectTimeout(Properties properties) {
    return connectTimeout(
        getProperty(properties, CONNECT_TIMEOUT_PROP_NAME, DEFAULT_CONNECT_TIMEOUT));
  }

  public int maxFrameLength() {
    return maxFrameLength;
  }

  public TransportConfig maxFrameLength(int maxFrameLength) {
    this.maxFrameLength = maxFrameLength;
    return this;
  }

  public TransportConfig maxFrameLength(Properties properties) {
    return maxFrameLength(
        getProperty(properties, MAX_FRAME_LENGTH_PROP_NAME, DEFAULT_MAX_FRAME_LENGTH));
  }

  public MessageCodec messageCodec() {
    return messageCodec;
  }

  public TransportConfig messageCodec(MessageCodec messageCodec) {
    this.messageCodec = messageCodec;
    return this;
  }

  /**
   * Reads the {@link MessageCodec} class name; when not set, the first {@code ServiceLoader}
   * provider (or JDK serialization) is used, see {@link MessageCodec#INSTANCE}.
   *
   * @param properties properties
   * @return this
   */
  public TransportConfig messageCodec(Properties properties) {
    final var className = getProperty(properties, MESSAGE_CODEC_PROP_NAME);
    return messageCodec(
        className != null
            ? newInstance(MESSAGE_CODEC_PROP_NAME, className, MessageCodec.class)
            : MessageCodec.INSTANCE);
  }

  public Function<String, String> addressMapper() {
    return addressMapper;
  }

  public TransportConfig addressMapper(Function<String, String> addressMapper) {
    this.addressMapper = addressMapper;
    return this;
  }

  public TransportFactory transportFactory() {
    return transportFactory;
  }

  public TransportConfig transportFactory(TransportFactory transportFactory) {
    this.transportFactory = transportFactory;
    return this;
  }

  /**
   * Reads the {@link TransportFactory} class name, e.g. {@code
   * io.scalecube.transport.netty.tcp.TcpTransportFactory}; when not set, the first {@code
   * ServiceLoader} provider is used (websocket, when transport-netty is on the classpath), see
   * {@link TransportFactory#INSTANCE}.
   *
   * @param properties properties
   * @return this
   */
  public TransportConfig transportFactory(Properties properties) {
    final var className = getProperty(properties, TRANSPORT_FACTORY_PROP_NAME);
    return transportFactory(
        className != null
            ? newInstance(TRANSPORT_FACTORY_PROP_NAME, className, TransportFactory.class)
            : TransportFactory.INSTANCE);
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
