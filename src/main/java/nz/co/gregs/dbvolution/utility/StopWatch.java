/*
 * Copyright 2023 Gregory Graham.
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
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author gregorygraham
 */
public class StopWatch implements Cloneable {

  private Instant startTime = Instant.now();
  private Instant endTime = null;
  private Duration duration;

  private StopWatch() {
  }

  public static StopWatch start() {
    return new StopWatch();
  }

  protected static StopWatch unstarted() {
    StopWatch unstarted = new StopWatch();
    unstarted.blank();
    return unstarted;
  }

  public void end() {
    if (endTime == null) {
      endTime = Instant.now();
      duration = Duration.between(startTime, endTime);
    }
  }

  public final void stop() {
    end();
  }

  public long splitTime() {
    return Instant.now().toEpochMilli() - startTime.toEpochMilli();
  }

  /**
   * The duration in milliseconds.
   *
   * @return the elapsed time in milliseconds
   */
  public long duration() {
    return duration(ChronoUnit.MILLIS);
  }

  /**
   * The duration in the time unit required.
   *
   * @param unit the unit to report in
   * @return the elapsed time in milliseconds
   */
  public long duration(TemporalUnit unit) {
    end();
    return duration.get(unit);
  }

  public void reset() {
    restart();
  }

  public void restart() {
    endTime = null;
    startTime = Instant.now();
    duration = null;
  }

  protected void blank() {
    endTime = null;
    startTime = null;
    duration = null;
  }

  public void report() {
    stop();
    System.out.println("DURATION: " + duration());
  }

  public void reportSplitTime() {
    System.out.println("SPLIT TIME: " + splitTime());
  }

  public <RESULT> RESULT time(Supplier<RESULT> supplier) {
    restart();
    RESULT result = supplier.get();
    stop();
    return result;
  }

  public void time(Runnable supplier) {
    restart();
    supplier.run();
    stop();
  }

  public Instant getStartTime() {
    return startTime;
  }

  public Instant getEndTime() {
    return endTime;
  }

  public StopWatch copy() {
    try {
      return (StopWatch) clone();
    } catch (CloneNotSupportedException ex) {
      Logger.getLogger(StopWatch.class.getName()).log(Level.SEVERE, null, ex);
    }
    return null;
  }

}
