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
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.CompanyLogo;
import nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogo;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class PrimaryKeylessTableTest extends AbstractTest {

	public PrimaryKeylessTableTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void linkIntoTableTest() throws SQLException {
		CarCompany carCompany = new CarCompany();
		LinkCarCompanyAndLogo link = new LinkCarCompanyAndLogo();
		DBQuery dbQuery = database.getDBQuery(carCompany, link);
		dbQuery.setBlankQueryAllowed(true);
		dbQuery.print();
		dbQuery = database.getDBQuery(new CompanyLogo(), link);
		dbQuery.setBlankQueryAllowed(true);
		dbQuery.print();
	}

	@Test
	public void linkThruTableTest() throws SQLException {
		CarCompany carCompany = new CarCompany();
		LinkCarCompanyAndLogo link = new LinkCarCompanyAndLogo();

		DBQuery dbQuery = database.getDBQuery(carCompany, link, new CompanyLogo());
		dbQuery.setBlankQueryAllowed(true);
		dbQuery.print();
	}

	@Test
	public void testCartesianJoinProtection() throws SQLException, Exception {
		try {
			DBQuery dbQuery = database.getDBQuery(new Marque(), new CompanyLogo());
			dbQuery.setBlankQueryAllowed(true);
			dbQuery.print();
			throw new Exception("Should have thrown an AccidentalCartesianJoinException here.");
		} catch (AccidentalCartesianJoinException ex) {
		}
	}

//    @Test
//    public void testAdHocRelations() throws SQLException, Exception {
//        final CompanyLogo companyLogo = new CompanyLogo();
//        Marque myMarqueRow = new Marque();
//        myMarqueRow.addRelationship(myMarqueRow.carCompany, companyLogo,companyLogo.carCompany);
//        DBQuery dbQuery = database.getDBQuery(myMarqueRow, companyLogo);
//        dbQuery.setBlankQueryAllowed(true);
//        dbQuery.print();
//    }
}
