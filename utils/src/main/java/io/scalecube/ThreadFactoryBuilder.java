package io.scalecube;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A ThreadFactory builder, providing any combination of these features:
 * <ul>
 * <li>whether threads should be marked as {@linkplain Thread#setDaemon daemon} threads
 * <li>a {@linkplain ThreadFactoryBuilder#setNameFormat naming format}
 * <li>a {@linkplain Thread#setPriority thread priority}
 * <li>an {@linkplain Thread#setUncaughtExceptionHandler uncaught exception handler}
 * <li>a {@linkplain ThreadFactory#newThread backing thread factory}
 * </ul>
 * If no backing thread factory is provided, a default backing thread factory is used as if by calling
 * {@code setThreadFactory(}{@link Executors#defaultThreadFactory()}{@code )}.
 */
public final class ThreadFactoryBuilder {
  private String nameFormat = null;
  private Boolean daemon = null;
  private Integer priority = null;
  private UncaughtExceptionHandler uncaughtExceptionHandler = null;
  private ThreadFactory backingThreadFactory = null;

  /**
   * Sets the naming format to use when naming threads ({@link Thread#setName}) which are created with this
   * ThreadFactory.
   *
   * @param nameFormat a {@link String#format(String, Object...)}-compatible format String, to which a unique integer
   *        (0, 1, etc.) will be supplied as the single parameter. This integer will be unique to the built instance of
   *        the ThreadFactory and will be assigned sequentially. For example, {@code "rpc-pool-%d"} will generate thread
   *        names like {@code "rpc-pool-0"}, {@code "rpc-pool-1"}, {@code "rpc-pool-2"}, etc.
   * @return this for the builder pattern
   */
  public ThreadFactoryBuilder setNameFormat(String nameFormat) {
    format(nameFormat, 0); // fail fast if the format is bad or null
    this.nameFormat = nameFormat;
    return this;
  }

  /**
   * Sets daemon or not for new threads created with this ThreadFactory.
   *
   * @param daemon whether or not new Threads created with this ThreadFactory will be daemon threads
   * @return this for the builder pattern
   */
  public ThreadFactoryBuilder setDaemon(boolean daemon) {
    this.daemon = daemon;
    return this;
  }

  /**
   * Returns a new thread factory using the options supplied during the building process. After building, it is still
   * possible to change the options used to build the ThreadFactory and/or build again. State is not shared amongst
   * built instances.
   *
   * @return the fully constructed {@link ThreadFactory}
   */
  public ThreadFactory build() {
    return build(this);
  }

  private static ThreadFactory build(ThreadFactoryBuilder builder) {
    final String nameFormat = builder.nameFormat;
    final Boolean daemon = builder.daemon;
    final Integer priority = builder.priority;
    final UncaughtExceptionHandler uncaughtExceptionHandler =
        builder.uncaughtExceptionHandler;
    final ThreadFactory backingThreadFactory =
        (builder.backingThreadFactory != null)
            ? builder.backingThreadFactory
            : Executors.defaultThreadFactory();
    final AtomicLong count = (nameFormat != null) ? new AtomicLong(0) : null;
    return runnable -> {
      Thread thread = backingThreadFactory.newThread(runnable);
      if (nameFormat != null) {
        thread.setName(format(nameFormat, count.getAndIncrement()));
      }
      if (daemon != null) {
        thread.setDaemon(daemon);
      }
      if (priority != null) {
        thread.setPriority(priority);
      }
      if (uncaughtExceptionHandler != null) {
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
      }
      return thread;
    };
  }

  private static String format(String format, Object... args) {
    return String.format(Locale.ROOT, format, args);
  }
}

