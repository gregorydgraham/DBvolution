/*
 * Copyright 2025 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.utility;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.internal.query.QueryTimeout;

/**
 *
 * @author gregorygraham
 */
public class Timeout {
  
	protected static final Logger LOGGER = Logger.getLogger(QueryTimeout.class.getName());

  public static Timeout NEVER = new Timeout() {

    @Override
    public long getAmountAsSingleUnit() {
      return Long.MAX_VALUE;
    }

    @Override
    public Timer startTimer() {
      return new Timeout.Timer() {
        @Override
        public boolean hasExpired() {
          return false;
        }
      };
    }
  };

  public static Timeout IMMEDIATELY = new Timeout() {
    
    @Override
    public long getAmountAsSingleUnit() {
      return 0l;
    }

    @Override
    public ChronoUnit getUnit() {
      return ChronoUnit.NANOS;
    }

    @Override
    public Timer startTimer() {
      return new Timeout.Timer() {
        @Override
        public boolean hasExpired() {
          return true;
        }
      };
    }
  };

  private Duration duration;

  /**
   * Creates a new Timeout with a duration of 10 seconds.
   *
   * <p>
   * This is only used for the NEVER and IMMEDIATELY variants.</p>
   *
   */
  private Timeout() {
    super();
    duration = Duration.ofMillis(10000l);
  }

  /**
   * Creates a new Timeout for the given duration.
   *
   * @param duration the duration required.
   */
  public Timeout(final Duration duration) {
    super();
    this.duration = duration;
  }

  /**
   * Creates a new Timeout with a duration of milliseconds.
   *
   * @param millis the duration in milliseconds.
   * @return a Timeout that will last for millis milliseconds
   */
  public static Timeout milliseconds(Long millis) {
    return new Timeout(Duration.of(millis, ChronoUnit.MILLIS));
  }

  /**
   * Creates a new Timeout with a duration of seconds.
   *
   * @param seconds the duration in seconds.
   * @return a Timeout that will last for the required seconds
   */
  public static Timeout seconds(Long seconds) {
    return new Timeout(Duration.of(seconds, ChronoUnit.SECONDS));
  }

  /**
   * Creates a new Timeout with never expire.
   *
   * @return the static NEVER variant of Timeout
   */
  public static Timeout never() {
    return NEVER;
  }

  /**
   * Creates a new Timeout with zero duration.
   *
   * @return the static IMMEDIATELY variant of Timeout
   */
  public static Timeout immediately() {
    return IMMEDIATELY;
  }

  public boolean isNever() {
    return NEVER.equals(this);
  }

  public boolean isImmediately() {
    return IMMEDIATELY.equals(this);
  }

  public void setTimeout(long timeoutInUnits, TemporalUnit temporalUnit) {
    duration = Duration.of(timeoutInUnits, temporalUnit);
  }

  public Timer startTimer() {
    return new Timer(duration);
  }

  private boolean hasNanos() {
    return duration.getNano()%1000 != 0;
  }

  /**
   * Convert the internal delay into single ChronoUnit and report that amount.
   *
   * @return the timeout as one single ChronoUnit
   */
  public long getAmountAsSingleUnit() {
    if (hasNanos()) {
      try {
        return toNanos();
      } catch (Exception ex) {
        return Long.MAX_VALUE;
      }
    }
    return duration.toMillis();
  }

  /**
   * Report the single ChronoUnit used by {@link #getAmountAsSingleUnit() }
   *
   * @return a ChronoUnit.
   */
  public ChronoUnit getUnit() {
    if (hasNanos()) {
      return ChronoUnit.NANOS;
    }
    return ChronoUnit.MILLIS;
  }
  
  static final long NANOS_PER_SECOND =  1000_000_000L;

  private long toNanos() {
    // Copied from Duration.toNanos because it was erring
    long tempSeconds = duration.getSeconds();
    long tempNanos = duration.getNano();
    if (tempSeconds < 0) {
      // change the seconds and nano value to
      // handle Long.MIN_VALUE case
      tempSeconds = tempSeconds + 1;
      tempNanos = tempNanos - NANOS_PER_SECOND;
    }
    long totalNanos = Math.multiplyExact(tempSeconds, NANOS_PER_SECOND);
    totalNanos = Math.addExact(totalNanos, tempNanos);
    if (totalNanos == 0){
      LOGGER.warning("ZERO LENGTH TIMEOUT");
    }
    return totalNanos;
  }

  public static class Timer {

    private final Instant start;
    private final Instant end;

    private Timer() {
      start = Instant.now();
      end = Instant.now();
    }

    private Timer(Duration duration) {
      this.start = Instant.now();
      this.end = Instant.now().plus(duration);
    }

    public Instant getStart() {
      return start;
    }

    public Instant getEnd() {
      return end;
    }

    public boolean hasExpired() {
      return Instant.now().isAfter(end);
    }

    public final boolean isRunning() {
      return !hasExpired();
    }
  }
}
