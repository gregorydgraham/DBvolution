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
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.is;
import org.junit.Assert;
import org.junit.Test;

public class NumberExpressionTest extends AbstractTest {

	public NumberExpressionTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testSimpleEquation() throws SQLException {
//        database.setPrintSQLBeforeExecuting(true);

		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.uidMarque).mod(2).is(0));
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(11));
		for (Marque marque : dbQuery.getAllInstancesOf(marq)) {
			Assert.assertThat(marque.uidMarque.getValue().intValue() % 2, is(0));
		}

	}

	@Test
	public void testSimpleEquationWithValue() throws SQLException {

		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(marq.column(marq.uidMarque).mod(2).is(0));
//		dbQuery.addCondition(NumberExpression.value(0).is(marq.column(marq.uidMarque).mod(2)));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		Assert.assertThat(allRows.size(), is(11));
		for (Marque marque : dbQuery.getAllInstancesOf(marq)) {
			Assert.assertThat(marque.uidMarque.getValue().intValue() % 2, is(0));
		}

	}

	@Test
	public void testAllArithmetic() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.plus(2)
				.minus(4)
				.times(6)
				.dividedBy(3)
				.mod(5)
				.is(0));
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
		for (DBQueryRow dBQueryRow : allRows) {
			Marque marque = dBQueryRow.get(marq);
			Assert.assertThat(
					marque.uidMarque.getValue().intValue(),
					Matchers.anyOf(
							is(1),
							is(4893101)
					)
			);
		}
	}

	@Test
	public void testBrackets() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.plus(2)
				.minus(4)
				.bracket()
				.times(6)
				.bracket()
				.dividedBy(3)
				.is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.plus(2)
				.minus(4)
				.bracket()
				.times(6)
				.bracket()
				.dividedBy(3)
				.is(-2));
		allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testBracketsInCondition() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).plus(2).minus(4).bracket().times(6).bracket().dividedBy(3).is(-2));
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.plus(2)
				.minus(4)
				.bracket()
				.times(6)
				.bracket()
				.dividedBy(3)
				.is(-2));
		allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testACOS() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.isBetween(-1, 1)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.arccos()
				.is(0)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testASIN() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.times(0.3)
				.arcsin()
				.isBetween(0.3, 0.31)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testATAN() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.arctan()
				.isBetween(0.78, 0.79)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testATAN2() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.arctan2(NumberExpression.value(2))
				.isBetween(0.46, 0.47)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testCotangent() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.cotangent()
				.isBetween(0.64, 0.65)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testCOSH() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.cosh()
				.isBetween(1.54, 1.55)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testTANH() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.isBetween(-1, 10)
		);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.tanh()
				.isLessThan(1)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testGreatestOf() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				NumberExpression.greatestOf(marq.column(marq.uidMarque), NumberExpression.value(900000), NumberExpression.value(800000))
				.is(marq.column(marq.uidMarque))
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(20));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(4893059));
	}

	@Test
	public void testLeastOf() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		ArrayList<NumberExpression> arrayList = new ArrayList<NumberExpression>();
		arrayList.add(marq.column(marq.uidMarque));
		arrayList.add(NumberExpression.value(900000));
		arrayList.add(NumberExpression.value(800000));
		dbQuery.addCondition(
				NumberExpression.leastOf(arrayList)
				.is(marq.column(marq.uidMarque))
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testAppend() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.append("$")
				.isLike("%2$")
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));

		dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.append(StringExpression.value("$"))
				.isLike("%2$")
		);
		allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(3));
	}

	@Test
	public void testIsEven() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.isEven()
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(11));
	}

	@Test
	public void testAbs() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.times(-2).abs().is(4)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testDecimalPart() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.dividedBy(2).bracket().decimalPart().is(0.5)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(11));
	}

	@Test
	public void testIntegerPart() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.dividedBy(2).integerPart().is(1)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
	}

	@Test
	public void testIsOdd() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque)
				.isOdd()
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(11));
	}

	@Test
	public void testIsNotNull() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.updateCount)
				.isNotNull()
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(20));
	}

	@Test
	public void testIsNull() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.updateCount)
				.isNull()
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
	}

	@Test
	public void testLeastOfNumberArray() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				NumberExpression.leastOf(1, 2, 3, 4, 5)
				.is(marq.column(marq.uidMarque))
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testCOS() throws SQLException {
		Marque marq = new Marque();
		DBQuery dbQuery = database.getDBQuery(marq);
		dbQuery.addCondition(
				marq.column(marq.uidMarque).minus(1).cos().is(1));
		List<DBQueryRow> allRows = dbQuery.getAllRows();
//        database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Marque marque = allRows.get(0).get(marq);
		Assert.assertThat(marque.uidMarque.getValue().intValue(), is(1));
	}

	@Test
	public void testEXP() throws SQLException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.addCondition(
				carCo.column(carCo.uidCarCompany).exp().times(1000).trunc().is(7389));
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		CarCompany carCompany = allRows.get(0).get(carCo);
		Assert.assertThat(carCompany.uidCarCompany.getValue().intValue(), is(2));
	}

	public static class degreeRow extends CarCompany {

		private static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber degrees = new DBNumber(this.column(this.uidCarCompany).degrees());
		@DBColumn
		DBNumber radians = new DBNumber(this.column(this.uidCarCompany).degrees().radians());
		@DBColumn
		DBNumber tangent = new DBNumber(this.column(this.uidCarCompany).degrees().tan());
	}

	@Test
	public void testDegrees() throws SQLException {
		degreeRow carCo = new degreeRow();
		DBQuery dbQuery = database.getDBQuery(carCo).setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);

		dbQuery.addCondition(
				carCo.column(carCo.uidCarCompany).degrees().tan().isGreaterThan(0));
		allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
		for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			Assert.assertThat(Math.tan(Math.toDegrees(carCompany.uidCarCompany.getValue().doubleValue())) > 0,
					is(true));
		}
	}

	@Test
	public void testRadians() throws SQLException {
		CarCompany carCo = new CarCompany();
		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.addCondition(carCo.column(carCo.uidCarCompany).degrees().radians().degrees().tan().isGreaterThan(0));
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(2));
		for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			Assert.assertThat(Math.tan(Math.toDegrees(carCompany.uidCarCompany.getValue().doubleValue())) > 0,
					is(true));
		}
	}

	@Test
	public void testLength() throws SQLException {
		CarCompany carCo = new CarCompany();
		carCo.uidCarCompany.permittedValues(carCo.column(carCo.name).length().minus(1));
		DBQuery dbQuery = database.getDBQuery(carCo);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			Assert.assertThat(carCompany.uidCarCompany.getValue().intValue(),
					is(4));
		}
	}

	@Test
	public void testLocationOf() throws SQLException {
		CarCompany carCo = new CarCompany();
		carCo.uidCarCompany.permittedValues(carCo.column(carCo.name).locationOf("ord"));
		DBQuery dbQuery = database.getDBQuery(carCo);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		for (CarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			Assert.assertThat(carCompany.uidCarCompany.getValue().intValue(),
					is(2));
		}
	}

	@Test
	public void testLocationOfAsDBRowField() throws SQLException {
		ExtendedCarCompany carCo = new ExtendedCarCompany();
//		carCo.uidCarCompany.permittedValues(carCo.column(carCo.name).locationOf("ord"));
		DBQuery dbQuery = database.getDBQuery(carCo);
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(4));
		for (ExtendedCarCompany carCompany : dbQuery.getAllInstancesOf(carCo)) {
			System.out.println("LOCATION OF 'ORD': " + carCompany.locationOfORD.getValue().intValue() + " == " + carCompany.name.stringValue().indexOf("ord"));
			Assert.assertThat(carCompany.locationOfORD.getValue().intValue(),
					is(Matchers.isOneOf(0, carCompany.name.stringValue().indexOf("ord") + 1)));
		}
	}

	public static class ExtendedCarCompany extends CarCompany {

		public static final long serialVersionUID = 1L;

		@DBColumn
		DBNumber locationOfORD = new DBNumber(this.column(this.name).locationOf("ord"));

		public ExtendedCarCompany() {
			super();
		}

	}

	@Test
	public void testChoose() throws SQLException {
		CarCompanyWithChoose carCo = new CarCompanyWithChoose();
		DBQuery dbQuery = database.getDBQuery(carCo);
		List<DBQueryRow> allRows = dbQuery.setBlankQueryAllowed(true).getAllRows();
		database.print(allRows);
		for (CarCompanyWithChoose carCompany : dbQuery.getAllInstancesOf(carCo)) {
			if (carCompany.uidCarCompany.intValue() <= 1) {
				Assert.assertThat(carCompany.chooseOnID.getValue(), is("too low"));
			} else if (carCompany.uidCarCompany.intValue() == 2) {
				Assert.assertThat(carCompany.chooseOnID.getValue(), is("ok"));
			} else if (carCompany.uidCarCompany.intValue() == 3) {
				Assert.assertThat(carCompany.chooseOnID.getValue(), is("high"));
			} else {
				Assert.assertThat(carCompany.chooseOnID.getValue(), is("too high"));
			}
		}
	}

	public static class CarCompanyWithChoose extends CarCompany {

		public static final long serialVersionUID = 1L;

		@DBColumn
		DBString chooseOnID = new DBString(this.column(this.uidCarCompany).choose("too low", "ok", "high", "too high"));

		public CarCompanyWithChoose() {
			super();
		}
	}

}
