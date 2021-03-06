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
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class AdHocRelationshipTest extends AbstractTest {

	public AdHocRelationshipTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testAdHocRelationship() throws SQLException {
//        Marque marque = new Marque();
//        CarCompany carCompany = new CarCompany();
//
//        marque.addRelationship(marque.name, carCompany, carCompany.name);
//        marque.ignoreAllForeignKeys();
//
//        DBQuery query = database.getDBQuery(carCompany, marque);
//        query.setBlankQueryAllowed(true);
//
//        List<DBQueryRow> allRows = query.getAllRows();
//        query.print();
//
//        assertTrue("There should only be a row for TOYOTA", allRows.size() == 1);
	}

//    @Test
//    public void testAdHocRelationshipWithOperator() throws SQLException{
//        Marque marque = new Marque();
//        CarCompany carCompany = new CarCompany();
//
//        marque.addRelationship(marque.name, carCompany, carCompany.name, new DBLikeCaseInsensitiveOperator(null));
//        marque.ignoreAllForeignKeys();
//
//        DBQuery query = database.getDBQuery(carCompany, marque);
//        query.setBlankQueryAllowed(true);
//        List<DBQueryRow> allRows = query.getAllRows();
//        query.print();
//
//        assertTrue("There should only be rows for FORD and TOYOTA", allRows.size() == 2);
//    }
}
