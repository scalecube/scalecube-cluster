package io.scalecube.transport;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Network Emulator is allowing to control link quality between endpoints in order to allow testing
 * of message loss, message delay, cluster partitions cluster recovery and other network related
 * conditions.
 *
 * <p><b>NOTE:</b> used for test purposes.
 *
 * <p><b>NOTE:</b> if emulator is disabled then side effect functions does nothing.
 */
public final class NetworkEmulator {

  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkEmulator.class);

  private volatile OutboundSettings defaultOutboundSettings = new OutboundSettings(0, 0);
  private volatile InboundSettings defaultInboundSettings = new InboundSettings(true);

  private final Map<Address, OutboundSettings> outboundSettings = new ConcurrentHashMap<>();
  private final Map<Address, InboundSettings> inboundSettings = new ConcurrentHashMap<>();

  private final AtomicLong totalMessageSentCount = new AtomicLong();
  private final AtomicLong totalOutboundMessageLostCount = new AtomicLong();
  private final AtomicLong totalInboundMessageLostCount = new AtomicLong();

  private final boolean enabled;

  private final Address address;

  /**
   * Creates new instance of network emulator.
   *
   * @param address local address
   * @param enabled either network emulator is enabled
   */
  NetworkEmulator(Address address, boolean enabled) {
    this.address = address;
    this.enabled = enabled;
  }

  //// OUTBOUND functions

  /**
   * Returns network outbound settings applied to the given destination.
   *
   * @param destination address of target endpoint
   * @return network outbound settings
   */
  public OutboundSettings outboundSettings(Address destination) {
    return outboundSettings.getOrDefault(destination, defaultOutboundSettings);
  }

  /**
   * Sets given network emulator outbound settings for specific destination.
   *
   * @param destination address of target endpoint
   * @param lossPercent loss in percents
   * @param meanDelay mean delay
   */
  public void outboundSettings(Address destination, int lossPercent, int meanDelay) {
    if (!enabled) {
      return;
    }
    OutboundSettings settings = new OutboundSettings(lossPercent, meanDelay);
    outboundSettings.put(destination, settings);
    LOGGER.debug("Set outbound settings {} from {} to {}", settings, address, destination);
  }

  /**
   * Sets default network emulator outbound settings.
   *
   * @param lossPercent loss in percents
   * @param meanDelay mean delay
   */
  public void setDefaultOutboundSettings(int lossPercent, int meanDelay) {
    if (!enabled) {
      return;
    }
    defaultOutboundSettings = new OutboundSettings(lossPercent, meanDelay);
    LOGGER.debug("Set default outbound settings {} for {}", defaultOutboundSettings, address);
  }

  /**
   * Blocks outbound messages to the given destinations.
   *
   * @param destinations collection of target endpoints where to apply
   */
  public void blockOutbound(Address... destinations) {
    blockOutbound(Arrays.asList(destinations));
  }

  /**
   * Blocks outbound messages to the given destinations.
   *
   * @param destinations collection of target endpoints where to apply
   */
  public void blockOutbound(Collection<Address> destinations) {
    if (!enabled) {
      return;
    }
    for (Address destination : destinations) {
      outboundSettings.put(destination, new OutboundSettings(100, 0));
    }
    LOGGER.debug("Blocked outbound from {} to {}", address, destinations);
  }

  /**
   * Unblocks outbound messages to given destinations.
   *
   * @param destinations collection of target endpoints where to apply
   */
  public void unblockOutbound(Address... destinations) {
    unblockOutbound(Arrays.asList(destinations));
  }

  /**
   * Unblocks outbound messages to given destinations.
   *
   * @param destinations collection of target endpoints where to apply
   */
  public void unblockOutbound(Collection<Address> destinations) {
    if (!enabled) {
      return;
    }
    destinations.forEach(outboundSettings::remove);
    LOGGER.debug("Unblocked outbound from {} to {}", address, destinations);
  }

  /** Unblock outbound messages to all destinations. */
  public void unblockOutboundAll() {
    if (!enabled) {
      return;
    }
    outboundSettings.clear();
    LOGGER.debug("Unblocked outbound from {} to all destinations", address);
  }

  /**
   * Returns total message sent count computed by network emulator.
   *
   * @return total message sent; 0 if emulator is disabled
   */
  public long totalMessageSentCount() {
    if (!enabled) {
      return 0;
    }
    return totalMessageSentCount.get();
  }

  /**
   * Returns total outbound message lost count computed by network emulator.
   *
   * @return total message lost; 0 if emulator is disabled
   */
  public long totalOutboundMessageLostCount() {
    if (!enabled) {
      return 0;
    }
    return totalOutboundMessageLostCount.get();
  }

  /**
   * Conditionally fails given outbound message onto given address with {@link
   * NetworkEmulatorException}.
   *
   * @param msg outbound message
   * @param address target address
   * @return mono message
   */
  public Mono<Message> tryFailOutbound(Message msg, Address address) {
    return Mono.defer(
        () -> {
          if (!enabled) {
            return Mono.just(msg);
          }
          totalMessageSentCount.incrementAndGet();
          // Emulate message loss
          boolean isLost = outboundSettings(address).evaluateLoss();
          if (isLost) {
            totalOutboundMessageLostCount.incrementAndGet();
            return Mono.error(
                new NetworkEmulatorException("NETWORK_BREAK detected, not sent " + msg));
          } else {
            return Mono.just(msg);
          }
        });
  }

  /**
   * Conditionally delays given outbound message onto given address.
   *
   * @param msg outbound message
   * @param address target address
   * @return mono message
   */
  public Mono<Message> tryDelayOutbound(Message msg, Address address) {
    return Mono.defer(
        () -> {
          if (!enabled) {
            return Mono.just(msg);
          }
          totalMessageSentCount.incrementAndGet();
          // Emulate message delay
          int delay = (int) outboundSettings(address).evaluateDelay();
          if (delay > 0) {
            return Mono.just(msg).delayElement(Duration.ofMillis(delay));
          } else {
            return Mono.just(msg);
          }
        });
  }

  //// INBOUND functions

  /**
   * Returns network inbound settings applied to the given destination.
   *
   * @param destination address of target endpoint
   * @return network inbound settings
   */
  public InboundSettings inboundSettings(Address destination) {
    return inboundSettings.getOrDefault(destination, defaultInboundSettings);
  }

  /**
   * Sets given network emulator inbound settings for specific destination.
   *
   * @param shallPass shallPass inbound flag
   * @param meanDelay mean delay
   */
  public void inboundSettings(Address destination, boolean shallPass) {
    if (!enabled) {
      return;
    }
    InboundSettings settings = new InboundSettings(shallPass);
    inboundSettings.put(destination, settings);
    LOGGER.debug("Set inbound settings {} from {} to {}", settings, address, destination);
  }

  /**
   * Sets default network emulator inbound settings.
   *
   * @param lossPercent loss in percents
   * @param meanDelay mean delay
   */
  public void setDefaultOutboundSettings(boolean shallPass) {
    if (!enabled) {
      return;
    }
    defaultInboundSettings = new InboundSettings(shallPass);
    LOGGER.debug("Set default inbound settings {} for {}", defaultInboundSettings, address);
  }

  /**
   * Blocks inbound messages to the given destinations.
   *
   * @param destinations collection of target endpoints where to apply
   */
  public void blockInbound(Address... destinations) {
    blockInbound(Arrays.asList(destinations));
  }

  /**
   * Blocks inbound messages to the given destinations.
   *
   * @param destinations collection of target endpoints where to apply
   */
  public void blockInbound(Collection<Address> destinations) {
    if (!enabled) {
      return;
    }
    for (Address destination : destinations) {
      inboundSettings.put(destination, new InboundSettings(false));
    }
    LOGGER.debug("Blocked inbound from {} to {}", address, destinations);
  }

  /**
   * Unblocks inbound messages to given destinations.
   *
   * @param destinations collection of target endpoints where to apply
   */
  public void unblockInbound(Address... destinations) {
    unblockInbound(Arrays.asList(destinations));
  }

  /**
   * Unblocks inbound messages to given destinations.
   *
   * @param destinations collection of target endpoints where to apply
   */
  public void unblockInbound(Collection<Address> destinations) {
    if (!enabled) {
      return;
    }
    destinations.forEach(inboundSettings::remove);
    LOGGER.debug("Unblocked inbound from {} to {}", address, destinations);
  }

  /** Unblocks inbound messages to all destinations. */
  public void unblockInboundAll() {
    if (!enabled) {
      return;
    }
    inboundSettings.clear();
    LOGGER.debug("Unblocked inbound from {} to all destinations", address);
  }

  /**
   * Returns total inbound message lost count computed by network emulator.
   *
   * @return total message lost; 0 if emulator is disabled
   */
  public long totalInboundMessageLostCount() {
    if (!enabled) {
      return 0;
    }
    return totalInboundMessageLostCount.get();
  }

  /**
   * This class contains settings and computations for the network outbound link to evaluate message
   * loss and message delay on outbound path. Parameters:
   *
   * <ul>
   *   <li>Percent of losing messages.
   *   <li>Mean network delays in milliseconds. Delays are emulated using exponential distribution
   *       of probabilities.
   * </ul>
   */
  public static final class OutboundSettings {

    private final int lossPercent;
    private final int meanDelay;

    /**
     * Constructor for outbound link settings.
     *
     * @param lossPercent loss in percent
     * @param mean mean dealy
     */
    public OutboundSettings(int lossPercent, int mean) {
      this.lossPercent = lossPercent;
      this.meanDelay = mean;
    }

    /**
     * Returns probability of message loss in percents.
     *
     * @return loss in percents
     */
    public int lossPercent() {
      return lossPercent;
    }

    /**
     * Returns mean network delay for message in milliseconds.
     *
     * @return mean delay
     */
    public int meanDelay() {
      return meanDelay;
    }

    /**
     * Indicator function telling is loss enabled.
     *
     * @return boolean indicating would loss occur
     */
    public boolean evaluateLoss() {
      return lossPercent > 0
          && (lossPercent >= 100 || ThreadLocalRandom.current().nextInt(100) < lossPercent);
    }

    /**
     * Evaluates network delay according to exponential distribution of probabilities.
     *
     * @return delay
     */
    public long evaluateDelay() {
      if (meanDelay > 0) {
        // Network delays (network delays). Delays should be emulated using exponential distribution
        // of probabilities.
        // log(1-x)/(1/mean)
        double x0 = ThreadLocalRandom.current().nextDouble();
        double y0 = -Math.log(1 - x0) * meanDelay;
        return (long) y0;
      }
      return 0;
    }

    @Override
    public String toString() {
      return "OutboundSettings{loss=" + lossPercent + ", delay=" + meanDelay + '}';
    }
  }

  /**
   * This class contains settings and computations for the network inbound link to evaluate message
   * blocking on inbound path. Parameters:
   *
   * <ul>
   *   <li>ShallPass boolean flag for controlling inbound messages.
   * </ul>
   */
  public static class InboundSettings {

    private final boolean shallPass;

    /**
     * Constructor for inbound link settings.
     *
     * @param shallPass shall pass flag
     */
    public InboundSettings(boolean shallPass) {
      this.shallPass = shallPass;
    }

    /**
     * Returns shallPass inbound flag.
     *
     * @return shallPass flag
     */
    public boolean shallPass() {
      return shallPass;
    }

    @Override
    public String toString() {
      return "InboundSettings{shallPass=" + shallPass + '}';
    }
  }
}
