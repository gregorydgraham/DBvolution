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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBGeometry2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author gregorygraham
 */
public class Point2DExpressionTest extends AbstractTest {

	final GeometryFactory geometryFactory = new GeometryFactory();

	public Point2DExpressionTest(Object testIterationName, DBDatabase db) throws SQLException {
		super(testIterationName, db);
		PointTestTable pointTestTable = new PointTestTable();

		db.preventDroppingOfTables(false);
		db.dropTableNoExceptions(pointTestTable);
		db.createTable(pointTestTable);

		pointTestTable.point.setValue(geometryFactory.createPoint(new Coordinate(2, 3)));
		db.insert(pointTestTable);

		pointTestTable = new PointTestTable();
		pointTestTable.point.setValue(geometryFactory.createPoint(new Coordinate(4, 6)));
		db.insert(pointTestTable);

		pointTestTable = new PointTestTable();
		pointTestTable.point.setValue(geometryFactory.createPoint(new Coordinate(0, 0)));
		db.insert(pointTestTable);
	}

	public static class PointTestTable extends DBRow {

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger point_id = new DBInteger();

		@DBColumn("point_col")
		public DBPoint2D point = new DBPoint2D();
	}

	@Test
	public void testValue() throws SQLException {
		System.out.println("value");
		Point point = geometryFactory.createPoint(new Coordinate(2.0, 3.0));
		final PointTestTable pointTestTable = new PointTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(Point2DExpression.value(point).is(pointTestTable.column(pointTestTable.point)));
		List<PointTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).point_id.intValue(), is(1));
	}

	@Test
	public void testGetQueryableDatatypeForExpressionValue() {
		System.out.println("getQueryableDatatypeForExpressionValue");
		Point2DExpression instance = new Point2DExpression();
		DBPoint2D result = instance.getQueryableDatatypeForExpressionValue();
		assertEquals(DBPoint2D.class, result.getClass());
	}

	@Test
	public void testCopy() {
		System.out.println("copy");
		Point2DExpression instance = new Point2DExpression();
		Point2DExpression result = instance.copy();
		assertEquals(instance, result);
	}

	@Test
	public void testIsAggregator() {
		System.out.println("isAggregator");
		Point2DExpression instance = new Point2DExpression();
		boolean expResult = false;
		boolean result = instance.isAggregator();
		assertEquals(expResult, result);
	}

	@Test
	public void testGetTablesInvolved() {
		System.out.println("getTablesInvolved");
		final PointTestTable pointTestTable = new PointTestTable();
		Point2DExpression instance = new Point2DExpression(pointTestTable.column(pointTestTable.point));
		Set<DBRow> result = instance.getTablesInvolved();
		Assert.assertThat(result.size(), is(1));
		DBRow[] resultArray = result.toArray(new DBRow[]{});
		DBRow aRow = resultArray[0];
		if (!(aRow instanceof PointTestTable)) {
			fail("Set should include PointTestTable");
		}
	}

	@Test
	public void testIsPurelyFunctional() {
		System.out.println("isPurelyFunctional");
		Point2DExpression instance = new Point2DExpression();
		boolean result = instance.isPurelyFunctional();
		Assert.assertThat(result, is(true));

		final PointTestTable pointTestTable = new PointTestTable();
		instance = new Point2DExpression(pointTestTable.column(pointTestTable.point));
		Assert.assertThat(instance.isPurelyFunctional(), is(false));
	}

	@Test
	public void testGetIncludesNull() {
		System.out.println("getIncludesNull");
		Point2DExpression instance = new Point2DExpression((Point) null);
		Assert.assertThat(instance.getIncludesNull(), is(true));

		final PointTestTable pointTestTable = new PointTestTable();
		instance = new Point2DExpression(pointTestTable.column(pointTestTable.point));
		Assert.assertThat(instance.getIncludesNull(), is(false));
	}

	@Test
	public void testStringResult() throws SQLException {
		System.out.println("stringResult");
		Point point = geometryFactory.createPoint(new Coordinate(2.0, 3.0));
		final PointTestTable pointTestTable = new PointTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(Point2DExpression.value(point).stringResult().is(pointTestTable.column(pointTestTable.point).stringResult()));
		List<PointTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).point_id.intValue(), is(1));
	}

	@Test
	public void testIs_Point() throws SQLException {
		System.out.println("is");
		Point point = geometryFactory.createPoint(new Coordinate(2.0, 3.0));
		final PointTestTable pointTestTable = new PointTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.point).is(point));
		List<PointTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).point_id.intValue(), is(1));
	}

	@Test
	public void testIs_Point2DResult() throws SQLException {
		System.out.println("is");
		Point point = geometryFactory.createPoint(new Coordinate(2.0, 3.0));
		final PointTestTable pointTestTable = new PointTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(Point2DExpression.value(point).is(pointTestTable.column(pointTestTable.point)));
		List<PointTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).point_id.intValue(), is(1));
	}

	@Test
	public void testGetX() throws SQLException {
		System.out.println("getX");
		final PointTestTable pointTestTable = new PointTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.point).getX().is(2));
		List<PointTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).point_id.intValue(), is(1));

		dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.point).getX().is(4));
		allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).point_id.intValue(), is(2));
	}

	@Test
	public void testGetY() throws SQLException {
		System.out.println("getY");
		final PointTestTable pointTestTable = new PointTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.point).getY().is(3));
		List<PointTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).point_id.intValue(), is(1));

		dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.point).getY().is(6));
		allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).point_id.intValue(), is(2));
	}

	@Test
	public void testDimension() throws SQLException {
		System.out.println("dimension");
		final PointTestTable pointTestTable = new PointTestTable();
		DBQuery dbQuery = database.getDBQuery(pointTestTable);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.point).dimension().is(0));
		List<PointTestTable> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		Assert.assertThat(allRows.size(), is(3));
	}
	
	public static class BoundingBoxTest extends PointTestTable{
		@DBColumn
		public DBString stringPoint = new DBString(this.column(this.point).stringResult().substringBetween("(", " "));
		@DBColumn
		public DBNumber getX = new DBNumber(this.column(this.point).getX());
		@DBColumn
		public DBNumber getY = new DBNumber(this.column(this.point).getY());
		@DBColumn
		public DBGeometry2D boundingBox = new DBGeometry2D(this.column(this.point).boundingBox());
		@DBColumn
		public DBBoolean getXis2 = new DBBoolean(this.column(this.point).getX().is(2));
		
	}

	@Test
	public void testBoundingBox() throws SQLException {
		System.out.println("boundingBox");
		final BoundingBoxTest pointTestTable = new BoundingBoxTest();
		DBQuery dbQuery = database.getDBQuery(pointTestTable).setBlankQueryAllowed(true);
		dbQuery.addCondition(pointTestTable.column(pointTestTable.point).getY().is(3));
		List<BoundingBoxTest> allRows = dbQuery.getAllInstancesOf(pointTestTable);
		database.print(allRows);
		Assert.assertThat(allRows.size(), is(1));
		Assert.assertThat(allRows.get(0).point_id.intValue(), is(1));
		Assert.assertThat(allRows.get(0).boundingBox.getGeometryValue().toText(), is("POLYGON ((2 3, 2 3, 2 3, 2 3, 2 3))"));
	}

}
