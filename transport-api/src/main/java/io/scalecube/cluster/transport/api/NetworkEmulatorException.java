package io.scalecube.cluster.transport.api;

/**
 * Exception which is thrown by network emulator on message loss.
 *
 */
public final class NetworkEmulatorException extends RuntimeException {

  public NetworkEmulatorException(String message) {
    super(message);
  }

  /**
   * No need for stack trace since those exceptions are not really an exceptions, but checked error conditions.
   */
  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }
}
