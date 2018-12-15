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
    if (host == null || host.isEmpty()) {
      throw new IllegalArgumentException("host must be present");
    }
    if (port < 0) {
      throw new IllegalArgumentException("port must be eq or greater than 0");
    }
    this.host = convertIfLocalhost(host);
    this.port = port;
  }

  /**
   * Parses given host:port string to create Address instance.
   *
   * @param hostandport must come in form {@code host:port}
   */
  public static Address from(String hostandport) {
    if (hostandport == null || hostandport.isEmpty()) {
      throw new IllegalArgumentException("host-and-port string must be present");
    }

    Matcher matcher = ADDRESS_FORMAT.matcher(hostandport);
    if (!matcher.find()) {
      throw new IllegalArgumentException("can't parse host-and-port string from: " + hostandport);
    }

    String host = matcher.group(1);
    if (host == null || host.isEmpty()) {
      throw new IllegalArgumentException("can't parse host from: " + hostandport);
    }

    int port;
    try {
      port = Integer.parseInt(matcher.group(2));
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("can't parse port from: " + hostandport, ex);
    }

    return new Address(host, port);
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

  /**
   * Checks whether given host string is one of localhost variants. i.e. {@code 127.0.0.1}, {@code
   * 127.0.1.1} or {@code localhost}, and if so - then node's public IP address will be resolved and
   * returned.
   *
   * @param host host string
   * @return local ip address if given host is localhost
   */
  private static String convertIfLocalhost(String host) {
    String result;
    switch (host) {
      case "localhost":
      case "127.0.0.1":
      case "127.0.1.1":
        result = getLocalIpAddress().getHostAddress();
        break;
      default:
        result = host;
    }
    return result;
  }

  /**
   * Returns host.
   *
   * @return host
   */
  public String host() {
    return host;
  }

  /**
   * Returns port.
   *
   * @return port
   */
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
}
