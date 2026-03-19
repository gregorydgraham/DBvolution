/*
 * Copyright 2026 Gregory Graham.
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
package nz.co.gregs.dbvolution.internal.database;

import java.time.ZonedDateTime;
import java.util.logging.Level;

/**
 *
 * @author gregorygraham
 */
public class LoggedException {

  private final Throwable exception;
  private final Level level;
  private final String message;
  private final Object[] messageItems;
  private final ZonedDateTime time = ZonedDateTime.now();

  public LoggedException(Throwable exception, Level level, String message, Object[] messageItems) {
    this.exception = exception;
    this.level = level;
    this.message = message;
    this.messageItems = messageItems;
  }

  /**
   * @return the exception
   */
  public Throwable getException() {
    return exception;
  }

  /**
   * @return the level
   */
  public Level getLevel() {
    return level;
  }

  /**
   * @return the message
   */
  public String getMessage() {
    return message;
  }

  /**
   * @return the messageItems
   */
  public Object[] getMessageItems() {
    return messageItems;
  }

  /**
   * @return the time
   */
  public ZonedDateTime getTime() {
    return time;
  }
  
}
