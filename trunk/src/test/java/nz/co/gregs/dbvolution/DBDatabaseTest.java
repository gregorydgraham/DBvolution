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
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Gregory Graham
 */
public class DBDatabaseTest extends AbstractTest {

	public DBDatabaseTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Before
	@SuppressWarnings("empty-statement")
	@Override
	public void setUp() throws Exception {
		setup(database);
	}

	@After
	@Override
	public void tearDown() throws Exception {
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new DropTable2TestClass());
		database.preventDroppingOfTables(true);
		super.tearDown();
	}

	@Test
	public void testCreateTable() throws SQLException {

		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableTestClass());
			database.preventDroppingOfTables(true);
		} catch (AutoCommitActionDuringTransactionException ex) {
			System.out.println("SETUP: CreateTableTestClass table not dropped, probably doesn't exist: " + ex.getMessage());
		}

		final CreateTableTestClass createTableTestClass = new CreateTableTestClass();
		database.createTable(createTableTestClass);
		System.out.println("CreateTableTestClass table created successfully");

		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new CreateTableTestClass());
			database.preventDroppingOfTables(true);
		} catch (AutoCommitActionDuringTransactionException ex) {
			System.out.println("SETUP: CreateTableTestClass table not dropped, probably doesn't exist: " + ex.getMessage());
		}
	}

	@Test
	public void testDropTableException() throws SQLException {
		try {
			database.createTable(new DropTable2TestClass());
		} catch (SQLException ex) {
			System.out.println("SETUP: DropTable2TestClass table not created, probably already exists" + ex.getMessage());
		} catch (AutoCommitActionDuringTransactionException ex) {
			System.out.println("SETUP: DropTable2TestClass table not created, because you are in a transaction.");
		}
		try {
			database.dropTable(new DropTable2TestClass());
			database.preventDroppingOfTables(true);
			database.dropTable(new DropTable2TestClass());
			throw new DBRuntimeException("Drop Table Method failed to throw a AccidentalDroppingOfTableException exception.");
		} catch (AccidentalDroppingOfTableException oops) {
			System.out.println("AccidentalDroppingOfTableException successfully thrown");
		}
	}

	@Test
	public void testDropTable() throws SQLException {
		try {
			database.createTable(new DropTableTestClass());
		} catch (SQLException ex) {
			System.out.println("SETUP: DropTableTestClass table not created, probably already exists" + ex.getMessage());
		} catch (AutoCommitActionDuringTransactionException ex) {
			System.out.println("SETUP: DropTableTestClass table not created, probably already exists" + ex.getMessage());
		}
		database.preventDroppingOfTables(false);
		database.dropTable(new DropTableTestClass());
		database.preventDroppingOfTables(true);
		System.out.println("DropTableTestClass table dropped successfully");
	}

	public static class CreateTableTestClass extends DBRow {

		public static final long serialVersionUID = 1L;

		@DBColumn
		DBString name = new DBString();
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
}
