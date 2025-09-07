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
package nz.co.gregs.dbvolution.transactions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.ExceptionThrownDuringTransaction;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * @author Gregory Graham
 */
public class DBTableTransactionTest extends AbstractTest {

	public DBTableTransactionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testInsertRowsSucceeds() throws SQLException, Exception {
		List<Marque> original = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getRowsByExample(new Marque());
		DBTable<Marque> transacted = database.doTransaction((dbDatabase) -> {
      try {
        Marque myTableRow1 = new Marque();
        DBTable<Marque> marques = DBTable.getInstance(dbDatabase, myTableRow1);
        myTableRow1.getUidMarque().setValue(999);
        myTableRow1.getName().setValue("TOYOTA");
        myTableRow1.getNumericCode().setValue(10);
        marques.insert(myTableRow1);
        marques.setBlankQueryAllowed(true).getAllRows();
        List<Marque> myTableRows = new ArrayList<>();
        myTableRows.add(new Marque(3, "False", 1246974, "", 3, "UV", "TVR", "", "Y", new Date(), 4, null));
        marques.insert(myTableRows);
        marques.getAllRows();
        return marques;
      }catch (SQLException ex) {
        throw new ExceptionThrownDuringTransaction(ex);
      }
    }, true);
    assertThat(transacted, is(not(nullValue())));
    assertThat(transacted.count(), is(24l));
		List<Marque> added = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getRowsByExample(new Marque());
		assertTrue("Length of list after insert should be longer than the original", added.size() == original.size() + 2);
	}

	@Test
	public void testInsertRowsFailure() throws SQLException {
		List<Marque> original = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getRowsByExample(new Marque());
    DBTable<Marque> transacted = null;
		try {
			database.setQuietExceptionsPreference(true);
			transacted = database.doTransaction((DB) -> {
        try {
          Marque myTableRow1 = new Marque();
          DBTable<Marque> marques = DBTable.getInstance(DB, myTableRow1);
          myTableRow1.getUidMarque().setValue(999);
          myTableRow1.getName().setValue("TOYOTA");
          myTableRow1.getNumericCode().setValue(10);
          marques.insert(myTableRow1);
          List<Marque> myTableRows = new ArrayList<>();
          myTableRows.add(new Marque(9999, "False", 1246974, "", 3, "UV", "TVR", "", "Y", new Date(), 4, null));
          marques.insert(myTableRows);
          // should cause an AccidentalBlankQueryException, and rollback the transaction
          marques.getAllRows();
          return marques;
        }catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException ex) {
          ex.printStackTrace();
          assertThat(ex, is(instanceOf(AccidentalBlankQueryException.class)));
          throw new ExceptionThrownDuringTransaction(ex);
        }
      }, true);
		} catch (SQLException | ExceptionThrownDuringTransaction e) {
		} finally {
			database.setQuietExceptionsPreference(false);
		}
    assertThat(transacted, is(nullValue()));
		List<Marque> added = database.getDBTable(new Marque()).setBlankQueryAllowed(true).getRowsByExample(new Marque());
		assertTrue("Length of list after insert should be the same as the original", added.size() == original.size());

	}
}
