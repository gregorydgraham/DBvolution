/*
 * Copyright 2021 Gregory Graham.
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
package nz.co.gregs.dbvolution.internal.query;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.utility.StringCheck;
import nz.co.gregs.dbvolution.utility.Timeout;

/**
 *
 * @author gregorygraham
 */
public class StatementDetails {

	private final String sql;
	private SQLException exception;
	private QueryIntention intention;

	private String label = "Unlabelled SQL";
	private boolean ignoreExceptions = false;
	private boolean withGeneratedKeys = false;
	private String namedPKColumn;
	private DBStatement activeStatement;
	private Timeout timeout;
  private DBRow[] tablesInvolved;
  private long rowCount;

	public StatementDetails(String label, QueryIntention intent, String sql, DBStatement statement, Timeout timeout, DBRow... rows) {
		this(label, intent, sql, null, false, false, "", statement, timeout, rows);
	}

	public StatementDetails copy() {
		return new StatementDetails(label, intention, sql, exception, withGeneratedKeys, ignoreExceptions, namedPKColumn, activeStatement, timeout, tablesInvolved);
	}

	public StatementDetails(String label, QueryIntention intent, String sql, SQLException except, boolean generatedKeys, boolean ignoreExceptions, String pkColumn, DBStatement statement, Timeout timeout, DBRow... rows) {
		this.label = label;
		this.sql = sql;
		this.intention = intent;
		this.exception = except;
		this.withGeneratedKeys = generatedKeys;
		this.ignoreExceptions = ignoreExceptions;
		this.namedPKColumn = pkColumn;
		this.activeStatement = statement;
    this.timeout = timeout;
    this.tablesInvolved = Arrays.copyOf(rows, rows.length);
	}

	public String getSql() {
		return sql;
	}

	public SQLException getException() {
		return exception;
	}

	public QueryIntention getIntention() {
		return intention;
	}

	public String getLabel() {
		return label;
	}

	public StatementDetails withLabel(String label) {
		this.label = label;
		return this;
	}

	public boolean isIgnoreExceptions() {
		return ignoreExceptions;
	}

	public final StatementDetails setIgnoreExceptions(boolean ignoreExceptions) {
		this.ignoreExceptions = ignoreExceptions;
    return this;
	}

	public boolean requiresGeneratedKeys() {
		return withGeneratedKeys;
	}

	public StatementDetails withGeneratedKeys() {
		this.withGeneratedKeys = true;
		return this;
	}

	/**
	 * Calls the appropriate execute method on the Statement and returns the
	 * boolean result.
	 *
	 * @param stmt the statement on which to execute this SQL command
	 * @throws SQLException database errors are propagated
	 */
	public void execute(Statement stmt) throws SQLException {
//    attemptCount++;
		if (StringCheck.isNotEmptyNorNull(namedPKColumn)) {
			stmt.execute(sql, new String[]{namedPKColumn});
		} else if (requiresGeneratedKeys()) {
			stmt.execute(sql, Statement.RETURN_GENERATED_KEYS);
		} else {
			stmt.execute(sql);
		}
	}

	/**
	 * Calls the appropriate executeUpdate method on the Statement and returns the
	 * boolean result.
	 *
	 * @param stmt the statement on which to execute this SQL command
   * @return 
	 * @throws SQLException database errors are propagated
	 */
	public long executeUpdate(Statement stmt) throws SQLException {
//    attemptCount++;
    long result;
		if (StringCheck.isNotEmptyNorNull(namedPKColumn)) {
			 result = stmt.executeLargeUpdate(sql, new String[]{namedPKColumn});
		} else if (requiresGeneratedKeys()) {
			 result = stmt.executeLargeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
		} else {
			 result = stmt.executeLargeUpdate(sql);
		}
    this.rowCount = result;
    return result;
	}

	public StatementDetails withException(SQLException exp2) {
		this.exception = exp2;
		return this;
	}

	public StatementDetails withNamedPKColumn(String primaryKeyForRetrievingGeneratedKeys) {
		namedPKColumn = primaryKeyForRetrievingGeneratedKeys;
		return this;
	}

	public StatementDetails withIntention(QueryIntention queryIntention) {
		this.intention = queryIntention;
		return this;
	}

	public DBStatement getDBStatement() {
		return activeStatement;
	}

	public void setDBStatement(DBStatement statement) {
		this.activeStatement = statement;
	}

	public void setTimeout(Timeout timeoutTime) {
		timeout = timeoutTime;
	}

	public Timeout getTimeout() {
		return timeout;
	}
  
  public DBRow[] getTablesInvolved(){
    return tablesInvolved;
  }

//  public int getAttemptCount() {
//    return attemptCount;
//  }

  public long getRowCount() {
    return rowCount;
  }
}
