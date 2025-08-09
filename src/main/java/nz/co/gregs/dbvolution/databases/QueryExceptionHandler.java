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
package nz.co.gregs.dbvolution.databases;

import nz.co.gregs.regexi.Regex;
import static nz.co.gregs.dbvolution.databases.DBDatabaseImplementation.ResponseToException;

/**
 *
 * @author gregorygraham
 */
class QueryExceptionHandler {

  static ResponseToException handle(QueryExceptionHandler[] knownCases, QueryIntention intent, Exception exception) {
    for (QueryExceptionHandler knownCase : knownCases) {
      ResponseToException response = knownCase.canRespond(intent, exception.getMessage());
      if (ResponseToException.NOT_HANDLED.equals(response)) {
        // NOT HANDLED so we need to continue
      } else {
        return response;
      }
      response = knownCase.canRespond(intent, exception.getLocalizedMessage());
      if (ResponseToException.NOT_HANDLED.equals(response)) {
        // NOT HANDLED so we need to continue
      } else {
        return response;
      }
    }
    return ResponseToException.NOT_HANDLED;
  }

  ResponseToException expectedResponse;
  Regex regex;
  QueryIntention[] intentions;

  QueryExceptionHandler(ResponseToException expectedResponse, Regex regex, QueryIntention... intentions) {
    this.expectedResponse = expectedResponse;
    this.intentions = intentions;
    this.regex = regex;
  }

  ResponseToException canRespond(QueryIntention intent, String message) {
    if (expectedResponse==null){
      // this is a malformed QueryExceptionHandler so treat it as NOT HANDLED
      return ResponseToException.NOT_HANDLED;
    }
    if ((intentions.length == 0 || intent.isOneOf(intentions)) && regex.matchesWithinString(message)) {
      return expectedResponse;
    } else {
      return ResponseToException.NOT_HANDLED;
    }
  }

}
