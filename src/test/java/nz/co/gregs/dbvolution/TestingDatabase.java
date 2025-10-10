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
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import nz.co.gregs.dbvolution.actions.*;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.databases.connections.DBConnection;
import nz.co.gregs.dbvolution.databases.settingsbuilders.H2MemorySettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.DBSQLException;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.internal.query.StatementDetails;
import nz.co.gregs.dbvolution.utility.Brake;

/**
 *
 * @author gregorygraham
 */
public class TestingDatabase extends H2MemoryDB {

  private static final long serialVersionUID = 1l;

  private final Brake controller = new Brake();

  private boolean failOnInsert = false;
  private boolean failOnUpdate = false;
  private boolean failOnCreateTable = false;
  private boolean failOnDelete = false;
  private boolean generateBrokenActions;
  private boolean returnIncorrectActionResults;

  public void setFailOnUpdate(boolean failOnUpdate) {
    this.failOnUpdate = failOnUpdate;
  }

  private TestingDatabase(H2MemorySettingsBuilder builder) throws SQLException {
    super(builder);
  }

  /**
   * Creates a new database with designated name and label.
   *
   * <p>
   * Great for we you just need to make a database and don't need to keep
   * it.</p>
   *
   * @param label the database label to be used internally to identify the
   * database (not related to the database name)
   * @return a database that takes a long time to synch
   * @throws SQLException if a database error occurs
   */
  public static TestingDatabase createDatabase(String label) throws SQLException {
    return new TestingDatabase(new H2MemorySettingsBuilder().setDatabaseName(label).setLabel(label));
  }

  /**
   * Creates a new database with random (UUID based) name and label.
   *
   * <p>
   * Great for when you just need to make a database and don't need to keep
   * it.</p>
   *
   * @return a new slow synching database
   * @throws SQLException if a database error occurs
   */
  public static TestingDatabase createANewRandomDatabase() throws SQLException {
    return createANewRandomDatabase("", "");
  }

  /**
   * Creates a new database with random (UUID based name).
   *
   * <p>
   * Great for we you just need to make a database and don't need to keep
   * it.</p>
   *
   * @param prefix the string to add before the database name and label
   * @param postfix the string to add after the database name and label
   * @return a new slow synching database
   * @throws SQLException if a database error occurs
   */
  public static TestingDatabase createANewRandomDatabase(String prefix, String postfix) throws SQLException {
    final H2MemorySettingsBuilder settings = new H2MemorySettingsBuilder().withUniqueDatabaseName();
    settings.setDatabaseName(prefix + settings.getDatabaseName() + postfix);
    settings.setLabel(settings.getDatabaseName());
    return new TestingDatabase(settings);
  }

  @Override
  public <R extends DBRow> DBTable<R> getDBTable(R example) {
    return SlowSynchingDBTable.getInstance(this, example, controller);
  }

  public Brake getBrake() {
    return controller;
  }

  public void setFailOnInsert(boolean failOnInsert) {
    this.failOnInsert = failOnInsert;
  }

  public void setGenerateBrokenActions(boolean generateBrokenInsertActions) {
    this.generateBrokenActions = generateBrokenInsertActions;
  }

  public void setFailOnCreateTable(boolean failOnCreateTable) {
    this.failOnCreateTable = failOnCreateTable;
  }

  public void setFailOnDelete(boolean failOnDelete) {
    this.failOnDelete = failOnDelete;
  }

  public void setReturnIncorrectActionResults(boolean incorrectResults) {
    this.returnIncorrectActionResults = incorrectResults;
  }

  @Override
  public DBStatement getDBStatement() throws SQLException {
    if (returnIncorrectActionResults) {
      return new IncorrectActionResultsDBStatement(this,getConnection());
    }
    return super.getDBStatement();
  }

  @Override
  public DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException {
    DBAction actualAction = action;
    if (failOnInsert && (action instanceof DBInsert)) {
      throw new SQLException("DELIBERATELY FAILING DURING INSERT");
    }
    if (generateBrokenActions && (action instanceof DBAction)) {
      actualAction = new BrokenAction(action);
    }
    if (failOnUpdate && (action instanceof DBUpdate)) {
      throw new SQLException("DELIBERATELY FAILING DURING UPDATE");
    }
    if (failOnCreateTable && (action instanceof DBCreateTable)) {
      throw new SQLException("DELIBERATELY FAILING DURING CREATE TABLE");
    }
    if (failOnDelete && (action instanceof DBDelete)) {
      throw new SQLException("DELIBERATELY FAILING DURING DELETE");
    }
    return super.executeDBAction(actualAction);
  }

  private static class SlowSynchingDBTable<E extends DBRow> extends DBTable<E> {

    private Brake brake;

    private SlowSynchingDBTable(DBDatabase database, E exampleRow) {
      super(database, exampleRow);
    }

    static <R extends DBRow> DBTable<R> getInstance(DBDatabase database, R example, Brake brake) {
      SlowSynchingDBTable<R> dbTable = new SlowSynchingDBTable<>(database, example);
      dbTable.brake = brake;
      return dbTable;
    }

    @Override
    public DBActionList insert(E newRow) throws SQLException {
      brake.checkBrake();
      return super.insert(newRow);
    }

    @Override
    public DBActionList update(E newRow) throws SQLException {
      brake.checkBrake();
      return super.update(newRow);
    }

    @Override
    protected DBActionList updateAnyway(E row) throws SQLException {
      brake.checkBrake();
      return super.updateAnyway(row);
    }

    @Override
    public DBActionList delete(Collection<E> oldRows) throws SQLException {
      brake.checkBrake();
      return super.delete(oldRows);
    }
  }

  private static class BrokenInsert extends DBInsert {

    private static final long serialVersionUID = 1L;

    public BrokenInsert(DBInsert action) {
      super(action.getRow());
    }

    @Override
    public DBActionList execute(DBDatabase db) throws SQLException, DBSQLException {
      throw new SQLException("Deliberate Error During DBInsert.execute()");
    }
  }

  private static class BrokenAction extends DBAction {

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
    
  }

  private static class IncorrectActionResultsDBStatement extends DBStatement {

    private long MIN_INDEX = 100000000l ;
    private long MAX_INDEX = Long.MAX_VALUE;

    public IncorrectActionResultsDBStatement(DBDatabase db, DBConnection connection) {
      super(db, connection);
    }

    @Override
    public long execute(StatementDetails details) throws SQLException {
      return ThreadLocalRandom.current().nextLong(MIN_INDEX, MAX_INDEX);
    }

    @Override
    public long execute(QueryIntention queryIntention, String sql) throws SQLException {
      return ThreadLocalRandom.current().nextLong(MIN_INDEX, MAX_INDEX);
    }

  }
}
