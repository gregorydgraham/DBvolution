/*
 * Copyright 2014 gregorygraham.
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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.*;
import org.junit.Assert;
import static org.hamcrest.Matchers.*;

import net.sourceforge.velai.TaskExecutor;
import net.sourceforge.velai.utils.ParallelTaskGroup;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;

/**
 *
 * @author gregorygraham
 */
public class DBScriptTest extends AbstractTest {

	public DBScriptTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	/**
	 * Test of implement method, of class DBScript.
	 *
	 * @throws java.lang.Exception
	 */
	@Test
	public void testImplement() throws Exception {
		System.out.println("test");
		List<Marque> allMarques = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		DBScript script = new ScriptThatAdds2Marques();
		DBActionList result = script.implement(database);
		List<Marque> allMarques2 = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(
				allMarques2.size(),
				is(allMarques.size() + 2));
	}

	/**
	 * Test of test method, of class DBScript.
	 *
	 * @throws java.lang.Exception
	 */
	@Test
	public void testTest() throws Exception {
		System.out.println("test");
		List<Marque> allMarques = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		DBScript script = new ScriptThatAdds2Marques();
		DBActionList result = script.test(database);
		List<Marque> allMarques2 = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(
				allMarques2.size(),
				is(allMarques.size()));
	}

	/**
	 * Test of test method, of class DBScript.
	 *
	 * @throws java.lang.Exception
	 */
	@Test
	public void testTestTransactionsAreIsolated() throws Exception {
		final ScriptTestTable scriptTestTable = new ScriptTestTable();
		final DBTable<ScriptTestTable> table = database.getDBTable(scriptTestTable);
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(scriptTestTable);
		database.preventDroppingOfTables(true);
		database.createTable(scriptTestTable);
		List<ScriptTestTable> origRows = table.setBlankQueryAllowed(true).getAllRows();

		ParallelTaskGroup<DBActionList> taskGroup;
		taskGroup = new ParallelTaskGroup<DBActionList>();
		taskGroup.add(new CallableTestScript(database));
		taskGroup.add(new CallableTestScript(database));
		taskGroup.add(new CallableTestScript(database));
		taskGroup.add(new CallableTestScript(database));
		taskGroup.add(new CallableTestScript(database));
		taskGroup.add(new CallableTestScript(database));
		taskGroup.add(new CallableTestScript(database));
		TaskExecutor.execute(taskGroup);

		List<ScriptTestTable> allRows = table.setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), is(origRows.size()));
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(scriptTestTable);
		database.preventDroppingOfTables(true);
	}

	@Test
	public void testImplementTransactionsAreIsolated() throws Exception {
		final ScriptTestTable scriptTestTable = new ScriptTestTable();
		final DBTable<ScriptTestTable> table = database.getDBTable(scriptTestTable);
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(scriptTestTable);
		database.preventDroppingOfTables(true);
		database.createTable(scriptTestTable);
		List<ScriptTestTable> origRows = table.setBlankQueryAllowed(true).getAllRows();

		ParallelTaskGroup<DBActionList> taskGroup;
		taskGroup = new ParallelTaskGroup<DBActionList>();
		taskGroup.add(new CallableImplementScript(database));
		taskGroup.add(new CallableImplementScript(database));
		taskGroup.add(new CallableImplementScript(database));
		taskGroup.add(new CallableImplementScript(database));
		taskGroup.add(new CallableImplementScript(database));
		taskGroup.add(new CallableImplementScript(database));
		taskGroup.add(new CallableImplementScript(database));
		TaskExecutor.execute(taskGroup);

		List<ScriptTestTable> allRows = table.setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), is(origRows.size()));
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(scriptTestTable);
		database.preventDroppingOfTables(true);
	}

	public class ScriptThatAdds2Marques extends DBScript {

		@Override
		public DBActionList script(DBDatabase db) throws Exception {
			DBActionList actions = new DBActionList();
			Marque myTableRow = new Marque();
			DBTable<Marque> marques = DBTable.getInstance(db, myTableRow);
			myTableRow.getUidMarque().setValue(999);
			myTableRow.getName().setValue("TOYOTA");
			myTableRow.getNumericCode().setValue(10);
			actions.addAll(marques.insert(myTableRow));
			marques.setBlankQueryAllowed(true).getAllRows();
			marques.print();

			List<Marque> myTableRows = new ArrayList<Marque>();
			myTableRows.add(new Marque(3, "False", 1246974, "", 3, "UV", "TVR", "", "Y", new Date(), 4, null));

			actions.addAll(marques.insert(myTableRows));

			marques.getAllRows();
			marques.print();
			return actions;
		}
	}

	public class ScriptThatAddsAndRemoves2Rows extends DBScript {

		@Override
		public DBActionList script(DBDatabase db) throws Exception {
			DBActionList actions = new DBActionList();
			ArrayList<ScriptTestTable> myTableRows = new ArrayList<ScriptTestTable>();
			
			ScriptTestTable myTableRow = new ScriptTestTable();
			myTableRows.add(myTableRow);
			DBTable<ScriptTestTable> table = DBTable.getInstance(db, myTableRow);

			List<ScriptTestTable> origRows = table.setBlankQueryAllowed(true).getAllRows();

			myTableRow.name.setValue("TOYOTA");
			actions.addAll(table.insert(myTableRow));

			List<ScriptTestTable> allRows = table.setBlankQueryAllowed(true).getAllRows();
			table.print();
			Assert.assertThat(allRows.size(), is(origRows.size() + 1));
			final ScriptTestTable newRow = new ScriptTestTable("False");
			myTableRows.add(newRow);

			actions.addAll(table.insert(newRow));

			allRows = table.setBlankQueryAllowed(true).getAllRows();
			table.print();
			Assert.assertThat(allRows.size(), is(origRows.size() + 2));

			table.getAllRows();
			table.print();
			table.delete(myTableRows);
			allRows = table.setBlankQueryAllowed(true).getAllRows();
			table.print();
			Assert.assertThat(allRows.size(), is(origRows.size()));

			return actions;
		}
	}

	public class CallableTestScript implements Callable<DBActionList> {

		private final DBScript script;
		private final DBDatabase database;

		public CallableTestScript(DBDatabase database) {
			this.script = new ScriptThatAddsAndRemoves2Rows();
			this.database = database;
		}

		@Override
		public DBActionList call() throws Exception {
			return script.test(database);
		}

	}

	public class CallableImplementScript implements Callable<DBActionList> {

		private final DBScript script;
		private final DBDatabase database;

		public CallableImplementScript(DBDatabase database) {
			this.script = new ScriptThatAddsAndRemoves2Rows();
			this.database = database;
		}

		@Override
		public DBActionList call() throws Exception {
			return script.implement(database);
		}

	}

	public static class ScriptTestTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBPrimaryKey
		@DBColumn
		@DBAutoIncrement
		DBInteger uid = new DBInteger();

		@DBColumn
		DBString name = new DBString();

		public ScriptTestTable(String aFalse) {
			super();
			name.setValue(aFalse);
		}

		public ScriptTestTable() {
			super();
		}

	}
}
