package io.scalecube;

/**
 * Static utility methods pertaining to instances of {@link Throwable}.
 */
public final class Throwables {
  private Throwables() {}


  /**
   * Propagates {@code throwable} as-is if it is an instance of {@link RuntimeException} or {@link Error}, or else as a
   * last resort, wraps it in a {@code RuntimeException} and then propagates. This method always throws an exception.
   * The {@code RuntimeException} return type is only for client code to make Java type system happy in case a return
   * value is required by the enclosing method. Example usage:
   * 
   * <pre>
   * T doSomething() {
   *   try {
   *     return someMethodThatCouldThrowAnything();
   *   } catch (IKnowWhatToDoWithThisException e) {
   *     return handle(e);
   *   } catch (Throwable t) {
   *     throw Throwables.propagate(t);
   *   }
   * }
   * </pre>
   *
   * @param throwable the Throwable to propagate
   * @return nothing will ever be returned; this return type is only for your convenience, as illustrated in the example
   *         above
   */
  public static UncheckedException propagate(Throwable throwable) {
    propagateIfPossible(throwable);
    throw new UncheckedException(throwable);
  }

  /**
   * Propagates {@code throwable} exactly as-is, if and only if it is an instance of {@link RuntimeException} or
   * {@link Error}. Example usage:
   * 
   * <pre>
   * try {
   *   someMethodThatCouldThrowAnything();
   * } catch (IKnowWhatToDoWithThisException e) {
   *   handle(e);
   * } catch (Throwable t) {
   *   Throwables.propagateIfPossible(t);
   *   throw new RuntimeException("unexpected", t);
   * }
   * </pre>
   */
  public static void propagateIfPossible(Throwable throwable) {
    propagateIfInstanceOf(throwable, Error.class);
    propagateIfInstanceOf(throwable, RuntimeException.class);
  }

  /**
   * Propagates {@code throwable} exactly as-is, if and only if it is an instance of {@code
   * declaredType}. Example usage:
   * 
   * <pre>
   * try {
   *   someMethodThatCouldThrowAnything();
   * } catch (IKnowWhatToDoWithThisException e) {
   *   handle(e);
   * } catch (Throwable t) {
   *   Throwables.propagateIfInstanceOf(t, IOException.class);
   *   Throwables.propagateIfInstanceOf(t, SQLException.class);
   *   throw Throwables.propagate(t);
   * }
   * </pre>
   */
  public static <X extends Throwable> void propagateIfInstanceOf(Throwable throwable, Class<X> declaredType) throws X {
    // Check for null is needed to avoid frequent JNI calls to isInstance().
    if (throwable != null && declaredType.isInstance(throwable)) {
      throw declaredType.cast(throwable);
    }
  }

  /**
   * Returns the innermost cause of {@code throwable}. The first throwable in a chain provides context from when the
   * error or exception was initially detected. Example usage:
   * 
   * <pre>
   * assertEquals("Unable to assign a customer id", Throwables.getRootCause(e).getMessage());
   * </pre>
   */
  public static Throwable getRootCause(Throwable throwable) {
    Throwable result = throwable;
    Throwable cause;
    while ((cause = throwable.getCause()) != null) {
      result = cause;
    }
    return result;
  }

  private static class UncheckedException extends RuntimeException {

    private UncheckedException(Throwable cause) {
      super(cause);
    }
  }
}
