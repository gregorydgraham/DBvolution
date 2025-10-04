/*
 * Copyright Error: on line 4, column 29 in Templates/Licenses/license-apache20.txt
 Expecting a date here, found: 15/03/2014 Gregory Graham.
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

import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.databases.settingsbuilders.H2MemorySettingsBuilder;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.CompanyLogo;
import nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogo;
import nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogoWithPreviousLink;
import nz.co.gregs.dbvolution.example.Marque;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class QueryGraphVisualisationTest {

//	private DBDatabase database;
	/**
	 * The QueryGraph can not be tested automatically so this class and method
	 * allows it to be tested easily.
	 *
	 * @param args args
	 * @throws java.lang.Exception java.lang.Exception
	 */
	public static void main(String[] args) throws Exception {
		DBDatabase database;
		database = new H2MemoryDB(
				new H2MemorySettingsBuilder()
						.setDatabaseName("dbvolutionTest")
						.setUsername("")
						.setPassword("")
		);

		//QueryGraphVisualisationTest myObject = new QueryGraphVisualisationTest();
		QueryGraphVisualisationTest.setup(database);
		final LinkCarCompanyAndLogoWithPreviousLink linkCarCompanyAndLogoWithPreviousLink = new LinkCarCompanyAndLogoWithPreviousLink();

		DBQuery dbQuery = database.getDBQuery(new CarCompany(), new Marque());
		dbQuery.addOptional(new CompanyLogo(), linkCarCompanyAndLogoWithPreviousLink);
		dbQuery.displayQueryGraph();

		tearDown(database);
	}

	private static void setup(DBDatabase database) throws Exception {
		database.setPreventDroppingOfTables(false)
            .dropTableNoExceptions(new Marque());
		database.createTable(new Marque());

		database.setPreventDroppingOfTables(false)
            .dropTableNoExceptions(new CarCompany());
		database.createTable(new CarCompany());

		database.setPreventDroppingOfTables(false)
            .dropTableNoExceptions(new CompanyLogo());
		database.createTable(new CompanyLogo());

		database.setPreventDroppingOfTables(false)
            .dropTableNoExceptions(new LinkCarCompanyAndLogo());
		database.createTable(new LinkCarCompanyAndLogo());

		database.setPreventDroppingOfTables(false)
            .dropTableNoExceptions(new LinkCarCompanyAndLogoWithPreviousLink());
		database.createTable(new LinkCarCompanyAndLogoWithPreviousLink());
	}

	private static void tearDown(DBDatabase database) {
		database.setPreventDroppingOfTables(false)
            .dropTableNoExceptions(new LinkCarCompanyAndLogoWithPreviousLink());
		database.setPreventDroppingOfTables(false)
            .dropTableNoExceptions(new LinkCarCompanyAndLogo());
		database.setPreventDroppingOfTables(false)
            .dropTableNoExceptions(new CompanyLogo());
		database.setPreventDroppingOfTables(false)
            .dropTableNoExceptions(new CarCompany());
		database.setPreventDroppingOfTables(false)
            .dropTableNoExceptions(new Marque());
	}

}
