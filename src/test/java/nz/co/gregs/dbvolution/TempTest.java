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
package nz.co.gregs.dbvolution;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.UnableToSynchronizeDatabase;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class TempTest extends AbstractTest {

  static final private Logger LOG = Logger.getLogger(TempTest.class.getName());

  public TempTest(Object testIterationName, Object db) {
    super(testIterationName, db);
  }
  
  @After
  public void cleanup() throws SQLException{
    database.setPreventAccidentalDeletingAllRowsFromTable(false);
    database.deleteAllRowsFromTable(new Marque());
  }

  @Test
  public synchronized void testCanSynchroniseSingleDatabaseBecauseOfInsertError() {
    // preparation for using temporary SQLite databases
    final String newSQLite1Filename = "target/testCanSynchroniseSingleDatabaseBecauseOfInsertError1.sqlite";
    final String newSQLite2Filename = "target/testCanSynchroniseSingleDatabaseBecauseOfInsertError2.sqlite";
    File newSQLite1File = new File(newSQLite1Filename);
    File newSQLite2File = new File(newSQLite2Filename);
    // and make sure we cleanup
    newSQLite1File.deleteOnExit();
    newSQLite2File.deleteOnExit();

    // make a cluster
    try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {

      // test that the database can synchronise when added
      assertThat(database.tableExists(new Marque()), is(true));
      assertThat(database.getCount(new Marque()), is(22l));
      assertThat(cluster.tableExists(new Marque()), is(true));
      assertThat(cluster.getCount(new Marque()), is(22l));
      cluster.addTrackedTable(new Marque());

//      cluster.setPrintSQLBeforeExecuting(true);
      // test we can add an SQLite DB the normal way
      SQLiteDB newSqlite1 = new SQLiteDB(newSQLite1File, "dbv", "testing");
      Instant start = Instant.now();
      long offset = 200000l; // 60s is 60,000 so 200,000 is well over what we need
      long tooFar = start.plus(offset, ChronoUnit.MILLIS).toEpochMilli();
      cluster.addDatabaseAndWait(newSqlite1);
      cluster.waitUntilSynchronised();
      long stop = Instant.now().toEpochMilli();
      System.out.println("DURATION: " + (stop - start.toEpochMilli()));
      assertThat(stop, is(lessThan(tooFar)));
//      newSqlite.setPrintSQLBeforeExecuting(true);
      assertThat(newSqlite1.tableExists(new Marque()), is(true));
      assertThat(newSqlite1.getCount(new Marque()), is(22l));

      // test we can add an SQLite DB
      SQLiteDB newSqlite2 = new SQLiteDB(newSQLite2File, "dbv", "testing");
      assertThat(newSqlite2.tableExists(new Marque()), is(false));
      start = Instant.now();
      offset = 200000l; // 60s is 60,000 so 200,000 is well over what we need
      tooFar = start.plus(offset, ChronoUnit.MILLIS).toEpochMilli();
      cluster.addDatabase(newSqlite2);
      try {
        cluster.waitUntilDatabaseIsSynchronised(newSqlite2, offset);
      } catch (UnableToSynchronizeDatabase ex) {
        ex.printStackTrace();
        Assert.fail("Failed to synchronise the database");
      }
      stop = Instant.now().toEpochMilli();
      System.out.println("DURATION: " + (stop - start.toEpochMilli()));
      assertThat(stop, is(lessThan(tooFar)));
//      newSqlite.setPrintSQLBeforeExecuting(true);
      assertThat(newSqlite2.tableExists(new Marque()), is(true));
      assertThat(newSqlite2.getCount(new Marque()), is(22l));

      // test we can add an H2 Memory DB
      H2MemoryDB newH2DB = H2MemoryDB.createANewRandomDatabase();
      assertThat(newH2DB.tableExists(new Marque()), is(false));
      cluster.addDatabase(newH2DB);
      start = Instant.now();
      tooFar = start.plus(offset, ChronoUnit.MILLIS).toEpochMilli();
      try {
        cluster.waitUntilDatabaseIsSynchronised(newH2DB, offset);
      } catch (UnableToSynchronizeDatabase ex) {
        ex.printStackTrace();
        Assert.fail("Failed to synchronise the database");
      }
      stop = Instant.now().toEpochMilli();
      System.out.println("DURATION: " + (stop - start.toEpochMilli()));
      assertThat(stop, is(lessThan(tooFar)));
//      newH2DB.setPrintSQLBeforeExecuting(true);
      assertThat(newH2DB.tableExists(new Marque()), is(true));
      assertThat(newH2DB.getCount(new Marque()), is(22l));

      // test that the H2 database can synchronise after unsynchronised
      Marque toyota = new Marque();
      toyota.name.permittedPattern("TOYOTA");
      Marque BYD = cluster.get(toyota).get(0);
      BYD.name.setValue("BYD");
      BYD.uidMarque.setValue(1138);
      assertThat(BYD.name.hasChanged(), is(true));
      // now insert into newH2DB to make it out of step with the cluster
      newH2DB.insert(BYD);
      newH2DB.setPrintSQLBeforeExecuting(true);
      // set the timing 
      start = Instant.now();
      tooFar = start.plus(offset, ChronoUnit.MILLIS).toEpochMilli();
      // Will notice that newH2DB can't insert the new "BYD", quarantine it, and then synchronise it 
      cluster.insert(BYD);
      try {
        cluster.waitUntilDatabaseIsSynchronised(newH2DB, offset);
      } catch (UnableToSynchronizeDatabase ex) {
        ex.printStackTrace();
        Assert.fail("Failed to synchronise the database: " + ex.getMessage());
      }
      stop = Instant.now().toEpochMilli();
      System.out.println("DURATION: " + (stop - start.toEpochMilli()));
      assertThat(stop, is(lessThan(tooFar)));
      assertThat(newH2DB.tableExists(new Marque()), is(true));
      assertThat(newH2DB.getCount(new Marque()), is(23l));

      // test that the H2 database can synchronise after unsynchronised
      toyota = new Marque();
      toyota.name.permittedPattern("TOYOTA");
      BYD = cluster.get(toyota).get(0);
      BYD.name.setValue("GREAT WALL");
      BYD.uidMarque.setValue(1139);
      assertThat(BYD.name.hasChanged(), is(true));
      // now insert into newH2DB to make it out of step with the cluster
      newH2DB.insert(BYD);
      newH2DB.setPrintSQLBeforeExecuting(true);
      // set the timing 
      start = Instant.now();
      tooFar = start.plus(offset, ChronoUnit.MILLIS).toEpochMilli();
      // Will notice that newH2DB can't insert the new "BYD", quarantine it, and then synchronise it 
      cluster.insert(BYD);
      try {
        cluster.waitUntilDatabaseIsSynchronised(newH2DB);
      } catch (UnableToSynchronizeDatabase ex) {
        ex.printStackTrace();
        Assert.fail("Failed to synchronise the database: " + ex.getMessage());
      }
      stop = Instant.now().toEpochMilli();
      System.out.println("DURATION: " + (stop - start.toEpochMilli()));
      assertThat(stop, is(lessThan(tooFar)));
      assertThat(newH2DB.tableExists(new Marque()), is(true));
      assertThat(newH2DB.getCount(new Marque()), is(24l));
    } catch (SQLException ex) {
      LOG.log(Level.SEVERE, "SQLException during test", ex);
      Assert.fail("SQLException should not have happened");
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "IOException (probably making a new SQLite DB) during test", ex);
      Assert.fail("IOException should not have happened");
    } finally {
    }
  }

  @Test
  public synchronized void testCanSynchroniseSingleDatabaseBecauseOfDeleteError() {
    // preparation for using temporary SQLite databases
    final String newSQLite1Filename = "target/testCanSynchroniseSingleDatabaseBecauseOfDeleteError1.sqlite";
    final String newSQLite2Filename = "target/testCanSynchroniseSingleDatabaseBecauseOfDeleteError2.sqlite";
    File newSQLite1File = new File(newSQLite1Filename);
    File newSQLite2File = new File(newSQLite2Filename);
    // and make sure we cleanup
    newSQLite1File.deleteOnExit();
    newSQLite2File.deleteOnExit();

    // make a cluster
    try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {

      // test that the database can synchronise when added
      assertThat(database.tableExists(new Marque()), is(true));
      assertThat(database.getCount(new Marque()), is(22l));
      assertThat(cluster.tableExists(new Marque()), is(true));
      assertThat(cluster.getCount(new Marque()), is(22l));
      cluster.addTrackedTable(new Marque());

//      cluster.setPrintSQLBeforeExecuting(true);
      // test we can add an SQLite DB the normal way
      SQLiteDB newSqlite1 = new SQLiteDB(newSQLite1File, "dbv", "testing");
      Instant start = Instant.now();
      long offset = 200000l; // 60s is 60,000 so 200,000 is well over what we need
      long tooFar = start.plus(offset, ChronoUnit.MILLIS).toEpochMilli();
      cluster.addDatabaseAndWait(newSqlite1);
      cluster.waitUntilSynchronised();
      long stop = Instant.now().toEpochMilli();
      System.out.println("DURATION: " + (stop - start.toEpochMilli()));
      assertThat(stop, is(lessThan(tooFar)));
//      newSqlite.setPrintSQLBeforeExecuting(true);
      assertThat(newSqlite1.tableExists(new Marque()), is(true));
      assertThat(newSqlite1.getCount(new Marque()), is(22l));

      // test we can add an SQLite DB
      SQLiteDB newSqlite2 = new SQLiteDB(newSQLite2File, "dbv", "testing");
      assertThat(newSqlite2.tableExists(new Marque()), is(false));
      start = Instant.now();
      offset = 200000l; // 60s is 60,000 so 200,000 is well over what we need
      tooFar = start.plus(offset, ChronoUnit.MILLIS).toEpochMilli();
      cluster.addDatabase(newSqlite2);
      try {
        cluster.waitUntilDatabaseIsSynchronised(newSqlite2, offset);
      } catch (UnableToSynchronizeDatabase ex) {
        ex.printStackTrace();
        Assert.fail("Failed to synchronise the database");
      }
      stop = Instant.now().toEpochMilli();
      System.out.println("DURATION: " + (stop - start.toEpochMilli()));
      assertThat(stop, is(lessThan(tooFar)));
//      newSqlite.setPrintSQLBeforeExecuting(true);
      assertThat(newSqlite2.tableExists(new Marque()), is(true));
      assertThat(newSqlite2.getCount(new Marque()), is(22l));

      // test we can add an H2 Memory DB
      H2MemoryDB newH2DB = H2MemoryDB.createANewRandomDatabase();
      assertThat(newH2DB.tableExists(new Marque()), is(false));
      cluster.addDatabase(newH2DB);
      start = Instant.now();
      tooFar = start.plus(offset, ChronoUnit.MILLIS).toEpochMilli();
      try {
        cluster.waitUntilDatabaseIsSynchronised(newH2DB, offset);
      } catch (UnableToSynchronizeDatabase ex) {
        ex.printStackTrace();
        Assert.fail("Failed to synchronise the database");
      }
      stop = Instant.now().toEpochMilli();
      System.out.println("DURATION: " + (stop - start.toEpochMilli()));
      assertThat(stop, is(lessThan(tooFar)));
//      newH2DB.setPrintSQLBeforeExecuting(true);
      assertThat(newH2DB.tableExists(new Marque()), is(true));
      assertThat(newH2DB.getCount(new Marque()), is(22l));

      // test that the H2 database can synchronise after unsynchronised
      Marque toyota = new Marque();
      toyota.name.permittedPattern("TOYOTA");
      Marque BYD = cluster.get(toyota).get(0);
      // now wipe make the H2 DB out of step with the cluster
      newH2DB.setPrintSQLBeforeExecuting(true);
      newH2DB.setPreventAccidentalDeletingAllRowsFromTable(false);
      newH2DB.deleteAllRowsFromTable(new Marque());
      // set the timing 
      start = Instant.now();
      tooFar = start.plus(offset, ChronoUnit.MILLIS).toEpochMilli();
      // Will notice that newH2DB no longer has any Marques, quarantine it, and then synchronise it 
      cluster.delete(BYD);
      try {
        cluster.waitUntilDatabaseIsSynchronised(newH2DB);
      } catch (UnableToSynchronizeDatabase ex) {
        ex.printStackTrace();
        Assert.fail("Failed to synchronise the database: " + ex.getMessage());
      }
      stop = Instant.now().toEpochMilli();
      System.out.println("DURATION: " + (stop - start.toEpochMilli()));
      assertThat(stop, is(lessThan(tooFar)));
      assertThat(newH2DB.tableExists(new Marque()), is(true));
      assertThat(newH2DB.getCount(new Marque()), is(21l));

    } catch (SQLException ex) {
      LOG.log(Level.SEVERE, "SQLException during test", ex);
      Assert.fail("SQLException should not have happened");
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "IOException (probably making a new SQLite DB) during test", ex);
      Assert.fail("IOException should not have happened");
    } finally {
    }
  }

  @Test
  public synchronized void testCanSynchroniseSingleDatabaseBecauseOfUpdateError() {
    // preparation for using temporary SQLite databases
    final String newSQLite1Filename = "target/testCanSynchroniseSingleDatabaseBecauseOfUpdateError1.sqlite";
    final String newSQLite2Filename = "target/testCanSynchroniseSingleDatabaseBecauseOfUpdateError2.sqlite";
    File newSQLite1File = new File(newSQLite1Filename);
    File newSQLite2File = new File(newSQLite2Filename);
    // and make sure we cleanup
    newSQLite1File.deleteOnExit();
    newSQLite2File.deleteOnExit();

    // make a cluster
    try (DBDatabaseCluster cluster = DBDatabaseCluster.randomManualCluster(database)) {

      // test that the database can synchronise when added
      assertThat(database.tableExists(new Marque()), is(true));
      assertThat(database.getCount(new Marque()), is(22l));
      assertThat(cluster.tableExists(new Marque()), is(true));
      assertThat(cluster.getCount(new Marque()), is(22l));
      cluster.addTrackedTable(new Marque());

//      cluster.setPrintSQLBeforeExecuting(true);
      // test we can add an SQLite DB the normal way
      SQLiteDB newSqlite1 = new SQLiteDB(newSQLite1File, "dbv", "testing");
      Instant start = Instant.now();
      long offset = 200000l; // 60s is 60,000 so 200,000 is well over what we need
      long tooFar = start.plus(offset, ChronoUnit.MILLIS).toEpochMilli();
      cluster.addDatabaseAndWait(newSqlite1);
      cluster.waitUntilSynchronised();
      long stop = Instant.now().toEpochMilli();
      System.out.println("DURATION: " + (stop - start.toEpochMilli()));
      assertThat(stop, is(lessThan(tooFar)));
//      newSqlite.setPrintSQLBeforeExecuting(true);
      assertThat(newSqlite1.tableExists(new Marque()), is(true));
      assertThat(newSqlite1.getCount(new Marque()), is(22l));

      // test we can add an SQLite DB
      SQLiteDB newSqlite2 = new SQLiteDB(newSQLite2File, "dbv", "testing");
      assertThat(newSqlite2.tableExists(new Marque()), is(false));
      start = Instant.now();
      offset = 200000l; // 60s is 60,000 so 200,000 is well over what we need
      tooFar = start.plus(offset, ChronoUnit.MILLIS).toEpochMilli();
      cluster.addDatabase(newSqlite2);
      try {
        cluster.waitUntilDatabaseIsSynchronised(newSqlite2, offset);
      } catch (UnableToSynchronizeDatabase ex) {
        ex.printStackTrace();
        Assert.fail("Failed to synchronise the database");
      }
      stop = Instant.now().toEpochMilli();
      System.out.println("DURATION: " + (stop - start.toEpochMilli()));
      assertThat(stop, is(lessThan(tooFar)));
//      newSqlite.setPrintSQLBeforeExecuting(true);
      assertThat(newSqlite2.tableExists(new Marque()), is(true));
      assertThat(newSqlite2.getCount(new Marque()), is(22l));

      // test we can add an H2 Memory DB
      H2MemoryDB newH2DB = H2MemoryDB.createANewRandomDatabase();
      assertThat(newH2DB.tableExists(new Marque()), is(false));
      cluster.addDatabase(newH2DB);
      start = Instant.now();
      tooFar = start.plus(offset, ChronoUnit.MILLIS).toEpochMilli();
      try {
        cluster.waitUntilDatabaseIsSynchronised(newH2DB, offset);
      } catch (UnableToSynchronizeDatabase ex) {
        ex.printStackTrace();
        Assert.fail("Failed to synchronise the database");
      }
      stop = Instant.now().toEpochMilli();
      System.out.println("DURATION: " + (stop - start.toEpochMilli()));
      assertThat(stop, is(lessThan(tooFar)));
//      newH2DB.setPrintSQLBeforeExecuting(true);
      assertThat(newH2DB.tableExists(new Marque()), is(true));
      assertThat(newH2DB.getCount(new Marque()), is(22l));

      // test that the H2 database can synchronise after unsynchronised
      Marque toyota = new Marque();
      toyota.name.permittedPattern("HONDA");
      Marque BYD = cluster.get(toyota).get(0);
      BYD.name.setValue(BYD.getName().getValue().toLowerCase());
      assertThat(BYD.name.hasChanged(), is(true));
      // now wipe make the H2 DB out of step with the cluster
      newH2DB.setPrintSQLBeforeExecuting(true);
      newH2DB.setPreventAccidentalDeletingAllRowsFromTable(false);
      newH2DB.deleteAllRowsFromTable(new Marque());
      // set the timing 
      start = Instant.now();
      tooFar = start.plus(offset, ChronoUnit.MILLIS).toEpochMilli();
      // Will notice that newH2DB no longer has any Marques, quarantine it, and then synchronise it 
      cluster.update(BYD);
      try {
        cluster.waitUntilDatabaseIsSynchronised(newH2DB);
      } catch (UnableToSynchronizeDatabase ex) {
        ex.printStackTrace();
        Assert.fail("Failed to synchronise the database: " + ex.getMessage());
      }
      stop = Instant.now().toEpochMilli();
      System.out.println("DURATION: " + (stop - start.toEpochMilli()));
      assertThat(stop, is(lessThan(tooFar)));
      assertThat(newH2DB.tableExists(new Marque()), is(true));
      assertThat(newH2DB.getCount(new Marque()), is(22l));
    } catch (SQLException ex) {
      LOG.log(Level.SEVERE, "SQLException during test", ex);
      Assert.fail("SQLException should not have happened");
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "IOException (probably making a new SQLite DB) during test", ex);
      Assert.fail("IOException should not have happened");
    } finally {
    }
  }

}
