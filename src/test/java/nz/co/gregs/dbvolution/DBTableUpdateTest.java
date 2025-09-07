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
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction;
import nz.co.gregs.dbvolution.generic.AbstractTest;

import org.junit.Test;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class DBTableUpdateTest extends AbstractTest {

	public DBTableUpdateTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}
  
	@Test
	public void changingPrimaryKey() throws SQLException, UnexpectedNumberOfRowsException, ExceptionThrownDuringTransaction {
    var script = new ScriptToUpdateToyotaPK();
    script.test(database);
    var marque = new Marque();
    marque.uidMarque.permittedValues(1,99999);
    assertThat(database.getDBQuery(marque).getOnlyInstanceOf(marque).name.getValue(), is("TOYOTA"));
  }
  
	@Test
	public void testUpdateSQL() throws SQLException, ExceptionThrownDuringTransaction {
    var marque = new Marque();
    marque.uidMarque.permittedValues(1);
    
    var script1 = new ScriptToCheckUpdateSQLButWithoutUsingDBTable();
    script1.test(database);
    assertThat(database.getDBQuery(marque).getOnlyInstanceOf(marque).name.getValue(), is("TOYOTA"));
    
    var script = new ScriptToCheckUpdateSQL();
    script.test(database);
    assertThat(database.getDBQuery(marque).getOnlyInstanceOf(marque).name.getValue(), is("TOYOTA"));
	}


	@Test
	public void changingPrimaryKeyViaDatabase() throws SQLException, UnexpectedNumberOfRowsException, ExceptionThrownDuringTransaction {
    var script = new ScriptToUpdateToyotaPK();
    database.test(script);
    var marque = new Marque();
    marque.uidMarque.permittedValues(1,99999);
    assertThat(database.getDBQuery(marque).getOnlyInstanceOf(marque).name.getValue(), is("TOYOTA"));
  }
  
	@Test
	public void testUpdateSQLViaDatabase() throws SQLException, ExceptionThrownDuringTransaction {
    var script = new ScriptToCheckUpdateSQL();
    database.test(script);
    var marque = new Marque();
    marque.uidMarque.permittedValues(1);
    assertThat(database.getDBQuery(marque).getOnlyInstanceOf(marque).name.getValue(), is("TOYOTA"));
	}

  private class ScriptToCheckUpdateSQL extends DBScript {

    public ScriptToCheckUpdateSQL() {
    }

    @Override
    public DBActionList script(DBDatabase scriptDB) throws Exception {
      
      Marque myTableRow = new Marque();
      myTableRow.getUidMarque().permittedValues(1); 
      DBTable<Marque> marquesTable = scriptDB.getDBTable(new Marque());
      marquesTable.getRowsByExample(myTableRow);
      
      Marque toyota = marquesTable.getFirstRow();
      
      assertThat("TOYOTA", is(toyota.name.toString()));
      
      toyota.name.setValue("NOTTOYOTA");
      DBActionList actions = marquesTable.update(toyota);
      String sqlForUpdate = actions.get(0).getSQLStatements(scriptDB).get(0);
      
      if (!(scriptDB instanceof DBDatabaseCluster)) {
        assertThat(testableSQL(sqlForUpdate),
                isIn(
                        new String[]{
                          testableSQL("UPDATE MARQUE SET NAME = 'NOTTOYOTA' WHERE (UID_MARQUE = 1);"),
                          testableSQL("UPDATE DBO.MARQUE SET NAME = N'NOTTOYOTA' WHERE (UID_MARQUE = 1);"),// SQLServer syntax
                          testableSQL("UPDATE MARQUE SET NAME = N'NOTTOYOTA' WHERE (UID_MARQUE = 1);")
                        }
                ));
      }
      actions.addAll(marquesTable.update(toyota));
      
      marquesTable.getRowsByExample(myTableRow);
      
      toyota = marquesTable.getFirstRow();
      assertThat("NOTTOYOTA", is(toyota.name.toString()));
      return actions;
    }
  }
  
  private class ScriptToCheckUpdateSQLButWithoutUsingDBTable extends DBScript {

    public ScriptToCheckUpdateSQLButWithoutUsingDBTable() {
    }

    @Override
    public DBActionList script(DBDatabase scriptDB) throws Exception {
      Marque myTableRow = new Marque();
      myTableRow.getUidMarque().permittedValues(1); 
      
      Marque toyota = scriptDB.getByExample(1l, myTableRow).get(0);
      
      assertThat("TOYOTA", is(toyota.name.toString()));
      
      toyota.name.setValue("NOTTOYOTA");
      DBActionList actions = scriptDB.update(toyota);
      String sqlForUpdate = actions.get(0).getSQLStatements(scriptDB).get(0);
      
      if (!(scriptDB instanceof DBDatabaseCluster)) {
        assertThat(testableSQL(sqlForUpdate),
                isIn(
                        new String[]{
                          testableSQL("UPDATE MARQUE SET NAME = 'NOTTOYOTA' WHERE (UID_MARQUE = 1);"),
                          testableSQL("UPDATE DBO.MARQUE SET NAME = N'NOTTOYOTA' WHERE (UID_MARQUE = 1);"),// SQLServer syntax
                          testableSQL("UPDATE MARQUE SET NAME = N'NOTTOYOTA' WHERE (UID_MARQUE = 1);")
                        }
                ));
      }
      actions.addAll(scriptDB.update(toyota));
      
      toyota = scriptDB.get(myTableRow).get(0);
      
      assertThat("NOTTOYOTA", is(toyota.name.toString()));
      return actions;
    }
  }

  private class ScriptToUpdateToyotaPK extends DBScript {

    public ScriptToUpdateToyotaPK() {
    }

    @Override
    public DBActionList script(DBDatabase scriptDB) throws Exception {
      
      Marque marqueExample = new Marque();
      marqueExample.getUidMarque().permittedValues(1);
      
      DBTable<Marque> marquesTable = scriptDB.getDBTable(new Marque());
      marquesTable.getRowsByExample(marqueExample);
      Marque toyota = marquesTable.getOnlyRowByExample(marqueExample);
      toyota.uidMarque.setValue(99999);
      DBActionList updateList = scriptDB.update(toyota);
      if (!(scriptDB instanceof DBDatabaseCluster)) {
        final String standardSQL = "UPDATE MARQUE SET UID_MARQUE = 99999 WHERE (UID_MARQUE = 1);";
        final String sqlserverSQL = "UPDATE dbo.MARQUE SET UID_MARQUE = 99999 WHERE (UID_MARQUE = 1);";
        final String oracleSQL = "update OO1081299805 set uid_marque = 99999 where (uid_marque = 1)";
        assertThat(testableSQL(updateList.get(0).getSQLStatements(scriptDB).get(0)),
                anyOf(
                        is(testableSQL(standardSQL)),
                        is(testableSQL(sqlserverSQL)),
                        is(testableSQL(oracleSQL))
                ));
      }
      
      scriptDB.update(toyota);
      toyota.name.setValue("NOTOYOTA");
      updateList = scriptDB.update(toyota);
      
      if (!(scriptDB instanceof DBDatabaseCluster)) {
        assertThat(testableSQL(updateList.get(0).getSQLStatements(scriptDB).get(0)),
                isIn(new String[]{
                  testableSQL("UPDATE MARQUE SET NAME = 'NOTOYOTA' WHERE (UID_MARQUE = 99999);"),
                  testableSQL("UPDATE DBO.MARQUE SET NAME = N'NOTOYOTA' WHERE (UID_MARQUE = 99999);"), //SQLSERVER syntax
                  testableSQL("UPDATE MARQUE SET NAME = N'NOTOYOTA' WHERE (UID_MARQUE = 99999);"),}));
      }
      marqueExample = new Marque();
      marqueExample.uidMarque.permittedValues(99999);
      toyota = marquesTable.getOnlyRowByExample(marqueExample);
      assertThat(toyota.name.toString(), is("NOTOYOTA"));
      return updateList;
    }
  }
}
