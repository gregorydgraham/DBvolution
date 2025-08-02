/*
/*
 * Copyright 2015 gregorygraham.
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
package nz.co.gregs.dbvolution.expressions;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.columns.JavaObjectColumn;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.Before;

/**
 *
 * @author gregorygraham
 */
public class JavaObjectExpressionTest extends AbstractTest {

	public JavaObjectExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Before
	public void before() throws AutoCommitActionDuringTransactionException, SQLException {
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new JavaObjectExpressionTable());
    
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new JavaObjectExpressionTable());
		database.createTable(new JavaObjectExpressionTable());
  
    var row = new JavaObjectExpressionTable(1,"toyota_logo.jpg",new SomeClass(4, "Testing is not null"));
    database.insert(row);

    row = new JavaObjectExpressionTable(2);
    database.insert(row);
    
    
    row = new JavaObjectExpressionTable(3,"Very testy",new SomeClass(0, "Very testy"));
    database.insert(row);
    
    if (database instanceof DBDatabaseCluster){
      // Because we're using a lot of blobs, 
      // we'll be doing about 4x as many updates
      // as inserts so we need to force a synch
      // onto the cluster.
      // this shouldn't be necessary for a normal
      // sitation.
      ((DBDatabaseCluster)database).waitUntilSynchronised(120000l);
    }
    
	}

	@Test
	public void testCopy() {
		var companyLogo = new JavaObjectExpressionTable();
		var instance = new JavaObjectExpression<>(companyLogo.column(companyLogo.someRandomClass));
		var result = instance.copy();
		final DBDefinition definition = database.getDefinition();
		assertEquals(instance.toSQLString(definition), result.toSQLString(definition));
	}

	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		var companyLogo = new JavaObjectExpressionTable();
		var instance = companyLogo.column(companyLogo.someRandomClass);
		DBJavaObject<SomeClass> expResult = new DBJavaObject<>();
		DBJavaObject<SomeClass> result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(expResult.getClass(), result.getClass());
	}

	@Test
	public void testIsAggregator() {
		JavaObjectExpression<SomeClass> instance = new JavaObjectExpression<>();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	@Test
	public void testGetTablesInvolved() {
		var companyLogo = new JavaObjectExpressionTable();
		var instance = new JavaObjectExpression<>(companyLogo.column(companyLogo.someRandomClass));
		Set<DBRow> result = instance.getTablesInvolved();
		DBRow[] resultArray = result.toArray(new DBRow[]{});
    assertThat(result.size(), is(1));
    assertThat(resultArray[0].getClass().getSimpleName(), is(companyLogo.getClass().getSimpleName()));
  }

  @Test
  public void testIsNotNull() throws SQLException, IOException {
    JavaObjectExpressionTable row = new JavaObjectExpressionTable();
    List<DBQueryRow> allRows = database.getDBQuery(row).setBlankQueryAllowed(true).setSortOrder(row.column(row.colInt)).getAllRows();
    assertThat(allRows.size(), is(3));
    
    final JavaObjectExpressionTable newRow = new JavaObjectExpressionTable();
    DBQuery dbQuery = database.getDBQuery(newRow).setBlankQueryAllowed(true);
    dbQuery.addCondition(newRow.column(newRow.someRandomClass).isNotNull());
    dbQuery.setSortOrder(newRow.column(newRow.colInt).ascending());
    
    allRows = dbQuery.getAllRows();
    
    assertThat(allRows.size(), is(2));
    assertThat(allRows.get(0).get(row).colInt.intValue(), is(1));
    assertThat(allRows.get(1).get(row).colInt.intValue(), is(3));
  }

  @Test
  public void testIsNull() throws SQLException, IOException {
    JavaObjectExpressionTable joTable = new JavaObjectExpressionTable();

    final JavaObjectExpressionTable newRow = new JavaObjectExpressionTable();

    DBQuery dbQuery = database.getDBQuery(newRow).setBlankQueryAllowed(true);
    dbQuery.addCondition(newRow.column(newRow.someRandomClass).isNull());
    dbQuery.setSortOrder(newRow.column(newRow.colInt).ascending());
    List<DBQueryRow> allRows = dbQuery.getAllRows();
    database.print(allRows);

    assertThat(allRows.size(), is(1));
    assertThat(allRows.get(0).get(joTable).colInt.intValue(), is(2));
  }

  @Test
  public void testGetIncludesNull() {
    JavaObjectExpression<SomeClass> instance = new JavaObjectExpression<>();
    boolean expResult = false;
		boolean result = instance.getIncludesNull();
		assertEquals(expResult, result);

		instance = new JavaObjectExpression<>(null);
		expResult = true;
		result = instance.getIncludesNull();
		assertEquals(expResult, result);
	}

	@Test
	public void testIsPurelyFunctional() {
		JavaObjectExpression<SomeClass> instance = new JavaObjectExpression<>();
		assertEquals(true, instance.isPurelyFunctional());

		var joTable = new JavaObjectExpressionTable();
		JavaObjectColumn<SomeClass> imageBytesColumn = joTable.column(joTable.someRandomClass);
		assertEquals(false, imageBytesColumn.isPurelyFunctional());
	}

	public static class JavaObjectExpressionTable extends DBRow {

		private static final long serialVersionUID = 1L;
		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger pkcolumn = new DBInteger();

		@DBColumn
		DBInteger colInt = new DBInteger();

		@DBColumn
		DBJavaObject<Integer> javaInteger = new DBJavaObject<Integer>();

		@DBColumn
		DBJavaObject<String> javaString = new DBJavaObject<String>();

		@DBColumn
		DBJavaObject<SomeClass> someRandomClass = new DBJavaObject<SomeClass>();

    public JavaObjectExpressionTable() {
    }

    public JavaObjectExpressionTable(int integer) {
      pkcolumn.setValue(integer);
      colInt.setValue(integer);
      javaInteger.setValue(integer);
    }

    public JavaObjectExpressionTable(int integer, String str, SomeClass someClass) {
      pkcolumn.setValue(integer);
      colInt.setValue(integer);
      javaInteger.setValue(integer);
      javaString.setValue(str);
      someRandomClass.setValue(someClass);
    }
	}

	public static class SomeClass implements Serializable {

		private static final long serialVersionUID = 1L;
		public String str;
		public int integer;

		public SomeClass(int integer, String str) {
			this.str = str;
			this.integer = integer;
		}
	}
}
