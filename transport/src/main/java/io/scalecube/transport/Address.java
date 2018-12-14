package io.scalecube.transport;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import reactor.core.Exceptions;

public final class Address {

  private static final Pattern ADDRESS_FORMAT = Pattern.compile("(?<host>^.*):(?<port>\\d+$)");

  private String host;
  private int port;

  /** Instantiates empty address for deserialization purpose. */
  Address() {}

  private Address(String host, int port) {
    requireNonEmpty(host);
    this.host = host;
    this.port = port;
  }

  /**
   * Parses given string to create address instance. For localhost variant host may come in: {@code
   * 127.0.0.1}, {@code localhost}; when localhost case detected then node's public IP address would
   * be resolved.
   *
   * @param hostAndPort must come in form {@code host:port}
   */
  public static Address from(String hostAndPort) {
    requireNonEmpty(hostAndPort);

    Matcher matcher = ADDRESS_FORMAT.matcher(hostAndPort);
    if (!matcher.find()) {
      throw new IllegalArgumentException();
    }

    String host = matcher.group(1);
    requireNonEmpty(host);
    String host1 =
        "localhost".equals(host) || "127.0.0.1".equals(host)
            ? getLocalIpAddress().getHostAddress()
            : host;
    int port = Integer.parseInt(matcher.group(2));
    return new Address(host1, port);
  }

  /** Creates address from host and port. */
  public static Address create(String host, int port) {
    return new Address(host, port);
  }

  /**
   * Getting local IP address by the address of local host. <b>NOTE:</b> returned IP address is
   * expected to be a publicly visible IP address.
   *
   * @throws RuntimeException wrapped {@link UnknownHostException} in case when local host name
   *     couldn't be resolved into an address.
   */
  public static InetAddress getLocalIpAddress() {
    try {
      return InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw Exceptions.propagate(e);
    }
  }

  /** Host address. */
  public String host() {
    return host;
  }

  /** Port. */
  public int port() {
    return port;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    Address that = (Address) other;
    return Objects.equals(host, that.host) && Objects.equals(port, that.port);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }

  @Override
  public String toString() {
    return host + ":" + port;
  }

  private static void requireNonEmpty(String string) {
    if (string == null || string.length() == 0) {
      throw new IllegalArgumentException();
    }
  }
}
