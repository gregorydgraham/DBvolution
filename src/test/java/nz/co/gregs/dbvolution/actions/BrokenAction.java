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
package nz.co.gregs.dbvolution.actions;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.DBSQLException;

/**
 *
 * @author gregorygraham
 */
public class BrokenAction extends DBAction {
  
  private static final long serialVersionUID = 1L;
  private final DBAction delegate;

  public BrokenAction(DBAction action) {
    super(action.getRow(), action.getIntent());
    this.delegate = action;
  }

  @Override
  public DBActionList execute(DBDatabase db) throws SQLException, DBSQLException {
    throw new SQLException("Deliberate Error During DBAction.execute()");
  }

  @Override
  public QueryIntention getIntent() {
    return delegate.getIntent();
  }

  @Override
  public long getRowsAltered() {
    return delegate.getRowsAltered();
  }

  @Override
  public DBRow getRow() {
    return delegate.getRow();
  }

  @Override
  public List<String> getSQLStatements(DBDatabase db) {
    return delegate.getSQLStatements(db);
  }

  @Override
  public boolean requiresRunOnIndividualDatabaseBeforeCluster() {
    return delegate.requiresRunOnIndividualDatabaseBeforeCluster();
  }

  @Override
  public boolean runOnDatabaseDuringCluster(DBDatabase initialDatabase, DBDatabase next) {
    return delegate.runOnDatabaseDuringCluster(initialDatabase, next);
  }

  @Override
  public DBActionList execute2(DBDatabase db) throws SQLException {
    throw new SQLException("Deliberate Error During DBAction.execute2()");
  }

  @Override
  public long getExpectedAlteredRows() {
    return delegate.getExpectedAlteredRows();
  }

  @Override
  public void setExpectedAlteredRows(long expectedResult) {
    delegate.setExpectedAlteredRows(expectedResult);
  }

  @Override
  protected DBActionList getRevertDBActionList() {
    return new DBActionList();
  }

  @Override
  protected DBActionList prepareActionList(DBDatabase db) throws SQLException, DBRuntimeException {
    return delegate.prepareActionList(db);
  }

  @Override
  protected void prepareRollbackData(DBDatabase db, DBActionList actions) throws SQLException, DBRuntimeException {
    delegate.prepareRollbackData(db, actions);
  }
  
}
