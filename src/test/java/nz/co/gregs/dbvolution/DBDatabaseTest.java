/*
 * Copyright 2013 Gregory Graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBRequiredTable;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfDatabaseException;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.regexi.Regex;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 *
 * @author Gregory Graham
 */
public class DBDatabaseTest extends AbstractTest {

  public DBDatabaseTest(Object testIterationName, Object db) {
    super(testIterationName, db);
  }

  @Before
  public synchronized void setUp() throws Exception {
    if (database instanceof DBDatabaseCluster) {
      ((DBDatabaseCluster) database).waitUntilSynchronised();
    }
    database.setPreventDroppingOfTables(false)
            .dropTableNoExceptions(new CreateTableWithForeignKeyTestClass());
  }

  @After
  @Override
  public synchronized void tearDown() throws Exception {
    database.setPreventDroppingOfTables(false)
            .dropTableNoExceptions(new DropTable2TestClass());
    super.tearDown();
  }

  @Test
  public void testCreateTable() throws SQLException {

    try {
      database.setPreventDroppingOfTables(false)
              .dropTableNoExceptions(new CreateTableTestClass());
    } catch (AutoCommitActionDuringTransactionException ex) {
//			SETUP: CreateTableTestClass table not dropped, probably doesn't exist
    }

    final CreateTableTestClass createTableTestClass = new CreateTableTestClass();
    database.createTable(createTableTestClass);
    assertThat(database.getDBTable(createTableTestClass).setBlankQueryAllowed(true).getAllRows().size(), is(0));

    try {
      database.setPreventDroppingOfTables(false)
              .dropTableNoExceptions(new CreateTableTestClass());
    } catch (AutoCommitActionDuringTransactionException ex) {
    }
  }

  @Test
  public void testTableExists() throws SQLException {
    final CreateTableTestClass createTableTestClass = new CreateTableTestClass();
    try {
      database.setPreventDroppingOfTables(false)
              .dropTableNoExceptions(createTableTestClass);
    } catch (AutoCommitActionDuringTransactionException ex) {
//			SETUP: CreateTableTestClass table not dropped, probably doesn't exist
    }
    assertThat(database.tableExists(createTableTestClass), is(false));

    database.createTable(createTableTestClass);
    assertThat(database.tableExists(createTableTestClass), is(true));
    assertThat(database.getDBTable(createTableTestClass).setBlankQueryAllowed(true).getAllRows().size(), is(0));

    try {
      database.setPreventDroppingOfTables(false)
              .dropTableNoExceptions(new CreateTableTestClass());
    } catch (AutoCommitActionDuringTransactionException ex) {
    }
    assertThat(database.tableExists(createTableTestClass), is(false));

  }

  @Test
  public void testTableExistsIsNotaffectedByMissingColumns() throws SQLException {
    final CreateTableTestClassWithOriginalColumns originalColumnTable = new CreateTableTestClassWithOriginalColumns();
    final CreateTableTestClassWithNewColumns newColumntable = new CreateTableTestClassWithNewColumns();
    try {
      database.setPreventDroppingOfTables(false)
              .dropTableNoExceptions(originalColumnTable);
    } catch (AutoCommitActionDuringTransactionException ex) {
//			SETUP: CreateTableTestClass table not dropped, probably doesn't exist
    }
    assertThat(database.tableExists(originalColumnTable), is(false));
    try {
      database.setPreventDroppingOfTables(false)
              .dropTableNoExceptions(originalColumnTable);
    } catch (AutoCommitActionDuringTransactionException ex) {
//			SETUP: CreateTableTestClass table not dropped, probably doesn't exist
    }
    assertThat(database.tableExists(originalColumnTable), is(false));

    database.createTable(originalColumnTable);
    assertThat(database.tableExists(newColumntable), is(true));

    try {
      database.createOrUpdateTable(newColumntable);
      database.setPreventDroppingOfTables(false)
              .dropTableNoExceptions(newColumntable);
    } catch (SQLException | AccidentalDroppingOfTableException | AutoCommitActionDuringTransactionException ex) {
      ex.printStackTrace();
    }
    assertThat(database.tableExists(originalColumnTable), is(false));

  }

  @Test
  public void testAddColumnToExistingTable() throws SQLException {
    // Any exceptions we don't expect are bad
    try {
      final CreateTableTestClassWithOriginalColumns originalColumnTable = new CreateTableTestClassWithOriginalColumns();
      final CreateTableTestClassWithNewColumns newColumntable = new CreateTableTestClassWithNewColumns();
      try {
        database.setPreventDroppingOfTables(false)
                .dropTableNoExceptions(originalColumnTable);
      } catch (AutoCommitActionDuringTransactionException ex) {
//			SETUP: CreateTableTestClass table not dropped, probably doesn't exist
      }
      assertThat(database.tableExists(originalColumnTable), is(false));
      // Create the original version
      database.createTable(originalColumnTable);
      // Check that it exists
      assertThat(database.tableExists(originalColumnTable), is(true));
      // makes sure we can insert into it
      originalColumnTable.id.setValue(5);
      originalColumnTable.name.setValue("FIVE");
      database.insert(originalColumnTable);
      // make sure that both versions of the table "exist"
      assertThat(database.tableExists(originalColumnTable), is(true));
      assertThat(database.tableExists(newColumntable), is(true));

      try{
      // Ensure we can't query on the new structure because there are missing
      // columns
      database.setQuietExceptionsPreference(true);
      SQLException except = Assert.assertThrows(SQLException.class, () -> database.getDBTable(newColumntable).setBlankQueryAllowed(true).getAllRows());
      Regex columnNotFound = Regex.startingAnywhere()
              .beginOrGroup()
              .literalCaseInsensitive("column")
              .anyCharacter().zeroOrMoreGreedy()
              .literalCaseInsensitive("not found")
              .or().literalCaseInsensitive("no such column")
              .or().literalCaseInsensitive("ora-00904")
              .or().literalCaseInsensitive("unknown column '").anythingButThis("'").literalCaseInsensitive("' in 'field list'")
              .or().literalCaseInsensitive("column ").anythingGreedy().literal("does not exist")
              .or().literalCaseInsensitive("invalid column name '").anythingButThis("'").literalCaseInsensitive("'.")
              .endOrGroup()
              .toRegex();
      if(!columnNotFound.matchesWithinString(except.getLocalizedMessage())){
        System.out.println("EXCEPTION MESSAGE: "+except.getLocalizedMessage().toLowerCase());
        except.printStackTrace();
      }
      Assert.assertTrue("Incorrect SQLException", columnNotFound.matchesWithinString(except.getLocalizedMessage()));
      }finally{
        database.setQuietExceptionsPreference(false);
      }

      // Update the table to the new specification
      database.createOrUpdateTable(newColumntable);
      // Retry the insert with the new structure
      List<CreateTableTestClassWithNewColumns> rows = database.getDBTable(newColumntable).setBlankQueryAllowed(true).getAllRows();
      assertThat(rows.size(), is(1));
      CreateTableTestClassWithNewColumns row = rows.get(0);
      assertThat(row.id.getValue(), is(5L));
      assertThat(row.name.getValue(), is("FIVE"));

      newColumntable.id.setValue(6);
      newColumntable.name.setValue("SIX");
      newColumntable.newColumn.setValue("SIX+1");
      database.insert(newColumntable);

      List<CreateTableTestClassWithNewColumns> newRows
              = database.getDBTable(new CreateTableTestClassWithNewColumns())
                      .setBlankQueryAllowed(true)
                      .setSortOrder(newColumntable.column(newColumntable.id))
                      .getAllRows();
      assertThat(newRows.size(), is(2));
      CreateTableTestClassWithNewColumns newRow = newRows.get(1);
      assertThat(newRow.id.getValue(), is(6L));
      assertThat(newRow.newColumn.getValue(), is("SIX+1"));
    } catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException except1) {
      except1.printStackTrace();
      Assert.fail("Should not have thrown an exception here");
    }
  }

  @Test
  public void testCreateTableWithForeignKeys() throws SQLException {

    try {
      database.setPreventDroppingOfTables(false)
              .dropTableNoExceptions(new CreateTableWithForeignKeyTestClass());
    } catch (AutoCommitActionDuringTransactionException ex) {
      //SETUP: CreateTableWithForeignKeyTestClass table not dropped, probably doesn't exist
    }
    try {
      database.setPreventDroppingOfTables(false)
              .dropTableNoExceptions(new CreateTableTestClass());
    } catch (AccidentalDroppingOfTableException | AutoCommitActionDuringTransactionException ex) {
    }

    final CreateTableTestClass createTableClass = new CreateTableTestClass();
    database.createTableWithForeignKeys(createTableClass);
    // This will throw an error if the table doesn't exist
    assertThat(database.getDBTable(createTableClass).setBlankQueryAllowed(true).getAllRows().size(), is(0));

    final CreateTableWithForeignKeyTestClass createTableTestClass = new CreateTableWithForeignKeyTestClass();
    database.createTableWithForeignKeys(createTableTestClass);
    // This will throw an error if the table doesn't exist
    assertThat(database.getDBTable(createTableTestClass).setBlankQueryAllowed(true).getAllRows().size(), is(0));
    database.createIndexesOnAllFields(createTableTestClass);

    try {
      database.setPreventDroppingOfTables(false)
              .dropTableNoExceptions(new CreateTableWithForeignKeyTestClass());
    } catch (AutoCommitActionDuringTransactionException ex) {
    }
  }

  @Test
  public void testCreateTableAndAddForeignKeys() throws SQLException {

    try {
      database.setPreventDroppingOfTables(false)
              .dropTableNoExceptions(new CreateTableWithForeignKeyTestClass2());
    } catch (AccidentalDroppingOfTableException | AutoCommitActionDuringTransactionException ex) {
      //SETUP: CreateTableWithForeignKeyTestClass table not dropped, probably doesn't exist
    }
    try {
      database.setPreventDroppingOfTables(false)
              .dropTableNoExceptions(new CreateTableTestClass2());
    } catch (AccidentalDroppingOfTableException | AutoCommitActionDuringTransactionException ex) {
    }

    final CreateTableTestClass2 createTableClass = new CreateTableTestClass2();
    database.createTable(createTableClass);
    assertThat(database.getDBTable(createTableClass).setBlankQueryAllowed(true).getAllRows().size(), is(0));

    final CreateTableWithForeignKeyTestClass2 createTableTestClass = new CreateTableWithForeignKeyTestClass2();
    database.createTable(createTableTestClass);
    assertThat(database.getDBTable(createTableTestClass).setBlankQueryAllowed(true).getAllRows().size(), is(0));

    database.createForeignKeyConstraints(createTableTestClass);

    database.createIndexesOnAllFields(createTableTestClass);

    try {
      database.setPreventDroppingOfTables(false)
              .dropTableNoExceptions(new CreateTableWithForeignKeyTestClass2());
    } catch (AutoCommitActionDuringTransactionException ex) {
    }
    try {
      database.setPreventDroppingOfTables(false)
              .dropTableNoExceptions(new CreateTableWithForeignKeyTestClass());
    } catch (AutoCommitActionDuringTransactionException ex) {
    }
  }

  @Test(expected = AccidentalDroppingOfTableException.class)
  public void testDropTableException() throws SQLException {
    database.setPreventDroppingOfTables(true);
    try {
      database.createTable(new DropTable2TestClass());
    } catch (SQLException | AutoCommitActionDuringTransactionException ex) {
      //SETUP: DropTable2TestClass table not created, probably already exists
    }
    //SETUP: DropTable2TestClass table not created, because you are in a transaction
    database.dropTable(new DropTable2TestClass());
  }

  @Test(expected = AccidentalDroppingOfTableException.class)
  public void testDropTableAllowedOnlyOnceException() throws SQLException {
    DBDatabase dangerous = database.setPreventDroppingOfTables(false);
    try {
      dangerous.createTable(new DropTable2TestClass());
    } catch (SQLException | AutoCommitActionDuringTransactionException ex) {
    }
    try {
      dangerous.dropTable(new DropTable2TestClass());
    } catch (AccidentalDroppingOfTableException oops) {
    }
    dangerous.createTable(new DropTable2TestClass());
    // will throw exception because only one drop table is allowed per call
    dangerous.dropTable(new DropTable2TestClass());
  }

  @Test(expected = RuntimeException.class)
  public void testDropTable() throws SQLException {
    try {
      database.createTable(new DropTableTestClass());
    } catch (SQLException | AutoCommitActionDuringTransactionException ex) {
      //SETUP: DropTableTestClass table not created, probably already exists
    }
    //SETUP: DropTableTestClass table not created, probably already exists
    //Prove that the table exists
    assertThat(database.getDBTable(new DropTableTestClass()).setBlankQueryAllowed(true).getAllRows().size(), is(0));
    database.setPreventDroppingOfTables(false)
            .dropTable(new DropTableTestClass());
    try {
      database.setQuietExceptionsPreference(true);
      assertThat(
              database
                      .getDBTable(new DropTableTestClass())
                      .setBlankQueryAllowed(true)
                      .getAllRows()
                      .size(),
              is(0)
      );
    } catch (SQLException exp) {
      throw new DBRuntimeException("Failed to assert that the table is empty", exp);
    } finally {
      database.setQuietExceptionsPreference(false);
    }
  }

  @Test(expected = AccidentalDroppingOfDatabaseException.class)
  public void testDropDatabaseException() throws SQLException, Exception {
    DBDatabase dangerous = database.setPreventDroppingOfTables(false);
    dangerous.preventDroppingOfDatabases(true);
    dangerous.dropDatabase(false);
  }

  @Test
  public void testIsLiterally() throws SQLException {
    Marque literalQuery = new Marque();
    literalQuery.getUidMarque().permittedValues(4893059);
    List<Marque> rowsByExample = database.getByExample(literalQuery);

    Assert.assertEquals(1, rowsByExample.size());
    Assert.assertEquals("" + 4893059, rowsByExample.get(0).getPrimaryKeys().get(0).toSQLString(database.getDefinition()));
  }

  @Test
  public void testIsLiterallyOnlyReturnsOne() throws SQLException, UnexpectedNumberOfRowsException {
    Marque literalQuery = new Marque();
    literalQuery.getUidMarque().permittedValues(4893059);
    List<Marque> rowsByExample = database.get(1l, literalQuery);

    Assert.assertEquals(1, rowsByExample.size());
    Assert.assertEquals("" + 4893059, rowsByExample.get(0).getPrimaryKeys().get(0).toSQLString(database.getDefinition()));
  }

  public void testRequiredTableAutomaticallyCreated() throws SQLException, Exception {
    Assert.assertTrue(database.tableExists(new RequiredTableShouldBeCreatedAutomatically()));
  }

  public static class CreateTableTestClass extends DBRow {

    public static final long serialVersionUID = 1L;

    @DBColumn
    @DBPrimaryKey
    @DBAutoIncrement
    DBInteger id = new DBInteger();

    @DBColumn
    DBString name = new DBString();
  }

  public static class CreateTableWithForeignKeyTestClass extends DBRow {

    public static final long serialVersionUID = 1L;

    @DBColumn
    DBString name = new DBString();

    @DBColumn
    @DBForeignKey(CreateTableTestClass.class)
    DBInteger marqueForeignKey = new DBInteger();
  }

  public static class CreateTableTestClass2 extends DBRow {

    public static final long serialVersionUID = 1L;

    @DBColumn
    @DBPrimaryKey
    @DBAutoIncrement
    DBInteger id = new DBInteger();

    @DBColumn
    DBString name = new DBString();
  }

  public static class CreateTableWithForeignKeyTestClass2 extends DBRow {

    public static final long serialVersionUID = 1L;

    @DBColumn
    DBString name = new DBString();

    @DBColumn
    @DBForeignKey(CreateTableTestClass2.class)
    DBInteger marqueForeignKey = new DBInteger();
  }

  public static class DropTableTestClass extends DBRow {

    public static final long serialVersionUID = 1L;

    @DBColumn
    DBString name = new DBString();
  }

  public static class DropTable2TestClass extends DBRow {

    public static final long serialVersionUID = 1L;

    @DBColumn
    DBString name = new DBString();
  }

  @DBRequiredTable()
  public static class RequiredTableShouldBeCreatedAutomatically extends DBRow {

    public static final long serialVersionUID = 1L;

    @DBColumn
    DBString name = new DBString();
  }

  @DBTableName("bert")
  public static class CreateTableTestClassWithOriginalColumns extends DBRow {

    public static final long serialVersionUID = 1L;

    @DBColumn
    @DBPrimaryKey
    @DBAutoIncrement
    DBInteger id = new DBInteger();

    @DBColumn
    DBString name = new DBString();
  }

  @DBTableName("bert")
  public static class CreateTableTestClassWithNewColumns extends DBRow {

    public static final long serialVersionUID = 1L;

    @DBColumn
    @DBPrimaryKey
    @DBAutoIncrement
    DBInteger id = new DBInteger();

    @DBColumn
    DBString name = new DBString();

    @DBColumn
    DBString newColumn = new DBString();
  }
}
