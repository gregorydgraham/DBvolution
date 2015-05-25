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
package nz.co.gregs.dbvolution.expressions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.example.*;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Gregory Graham
 */
public class ExistsExpressionTest extends AbstractTest {

	public ExistsExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testDBExistsOperator() throws SQLException {

		CarCompany carCompany = new CarCompany();
		carCompany.uidCarCompany.permittedValues(3);

		Marque marque = new Marque();
		DBQuery marquesQuery = database.getDBQuery(marque);
		marquesQuery.addCondition(new ExistsExpression(marque, carCompany));

		List<Marque> rowList = marquesQuery.getAllInstancesOf(marque);
		database.print(rowList);
		Assert.assertThat(rowList.size(), is(3));

		marque = new Marque();
		marquesQuery = database.getDBQuery(marque);
		marquesQuery.addCondition((new ExistsExpression(marque, carCompany)).not());

		marquesQuery.getAllInstancesOf(marque);
		rowList = marquesQuery.getAllInstancesOf(marque);
		database.print(rowList);
		Assert.assertThat(rowList.size(), is(19));
	}

	@Test
	public void testDBExistsOnMultipleTablesOperator() throws SQLException {

		CarCompany carCompany = new CarCompany();
		carCompany.uidCarCompany.permittedValues(3);
		ArrayList<DBRow> existsTables = new ArrayList<DBRow>();
		existsTables.add(carCompany);
		existsTables.add(new CompanyLogo());
//		DBExistsOperator carCompanyExists = new DBExistsOperator(carCompany, new CompanyLogo());

		Marque marque = new Marque();

		DBQuery marquesQuery = database.getDBQuery(marque);
		marquesQuery.addCondition(new ExistsExpression(marque, existsTables));

		List<Marque> rowList = marquesQuery.getAllInstancesOf(marque);
		database.print(rowList);
		Assert.assertThat(rowList.size(), is(0));

		CompanyLogo companyLogo = new CompanyLogo();
		companyLogo.carCompany.setValue(3);
		companyLogo.logoID.setValue(4);
		database.insert(companyLogo);

		rowList = marquesQuery.getAllInstancesOf(marque);
		database.print(rowList);
		Assert.assertThat(rowList.size(), is(3));

		marquesQuery = database.getDBQuery(marque);
		marquesQuery.addCondition((new ExistsExpression(marque, existsTables)).not());
		rowList = marquesQuery.getAllInstancesOf(marque);
		database.print(rowList);
		Assert.assertThat(rowList.size(), is(19));
	}

	@Test
	public void testDBExistsOnMultipleTablesUsingDBQueries() throws SQLException {

		CarCompany carCompany = new CarCompany();
		carCompany.uidCarCompany.permittedValues(3);
		DBQuery existsTables = database.getDBQuery();
		existsTables.add(carCompany);
		existsTables.add(new CompanyLogo());
//		DBExistsOperator carCompanyExists = new DBExistsOperator(carCompany, new CompanyLogo());

		Marque marque = new Marque();
		DBQuery outerQuery = database.getDBQuery(marque);

		DBQuery marquesQuery = database.getDBQuery(marque);
		marquesQuery.addCondition(new ExistsExpression(outerQuery, existsTables));

		List<Marque> rowList = marquesQuery.getAllInstancesOf(marque);
		database.print(rowList);
		Assert.assertThat(rowList.size(), is(0));

		CompanyLogo companyLogo = new CompanyLogo();
		companyLogo.carCompany.setValue(3);
		companyLogo.logoID.setValue(4);
		database.insert(companyLogo);

		rowList = marquesQuery.getAllInstancesOf(marque);
		database.print(rowList);
		Assert.assertThat(rowList.size(), is(3));

		marquesQuery = database.getDBQuery(marque);
		marquesQuery.addCondition((new ExistsExpression(outerQuery, existsTables)).not());
		rowList = marquesQuery.getAllInstancesOf(marque);
		database.print(rowList);
		Assert.assertThat(rowList.size(), is(19));
	}
}
