package io.scalecube.rsocket.transport;

public class ConnectionClosedException extends RuntimeException {

  public ConnectionClosedException() {}

  public ConnectionClosedException(String message) {
    super(message);
  }

  public ConnectionClosedException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConnectionClosedException(Throwable cause) {
    super(cause);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{errorMessage=" + getMessage() + '}';
  }
}
