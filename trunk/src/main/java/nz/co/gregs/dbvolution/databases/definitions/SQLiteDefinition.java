/*
 * Copyright 2014 Gregory Graham.
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
package nz.co.gregs.dbvolution.databases.definitions;

import com.vividsolutions.jts.geom.Polygon;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.datatypes.DBBooleanArray;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLine2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;
import nz.co.gregs.dbvolution.generation.DBTableField;
import nz.co.gregs.dbvolution.internal.datatypes.DateRepeatImpl;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.internal.sqlite.DateRepeatFunctions;
import nz.co.gregs.dbvolution.internal.sqlite.Line2DFunctions;
import nz.co.gregs.dbvolution.internal.sqlite.Point2DFunctions;
import nz.co.gregs.dbvolution.internal.sqlite.Polygon2DFunctions;
import org.joda.time.Period;

/**
 * Defines the features of the SQLite database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link SQLiteDB} instances,
 * and you should not need to use it directly.
 *
 * @author Gregory Graham
 */
public class SQLiteDefinition extends DBDefinition {

	private static final DateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");

	public static String SPATIAL_LINE_MAX_X_COORD_FUNCTION = "DBV_LINE_MAX_X2D_COORD";
	public static String SPATIAL_LINE_MIN_X_COORD_FUNCTION = "DBV_LINE_MIN_X2D_COORD";
	public static String SPATIAL_LINE_MAX_Y_COORD_FUNCTION = "DBV_LINE_MAX_Y2D_COORD";
	public static String SPATIAL_LINE_MIN_Y_COORD_FUNCTION = "DBV_LINE_MIN_Y2D_COORD";

	@Override
	public String getDateFormattedForQuery(Date date) {
		return " strftime('%Y-%m-%d %H:%M:%f', '" + DATETIME_FORMAT.format(date) + "') ";
	}

	@Override
	public boolean supportsGeneratedKeys() {
		return false;
	}

	@Override
	public String formatTableName(DBRow table) {
		return super.formatTableName(table).toUpperCase();
	}

	@Override
	public String getDropTableStart() {
		return super.getDropTableStart() + " IF EXISTS "; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean prefersTrailingPrimaryKeyDefinition() {
		return false;
	}

	@Override
	public String getColumnAutoIncrementSuffix() {
		return " PRIMARY KEY AUTOINCREMENT ";
	}

	@Override
	protected String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBLargeObject) {
			return " TEXT ";
		} else if (qdt instanceof DBBooleanArray) {
			return " VARCHAR(64) ";
		} else if (qdt instanceof DBDate) {
			return " DATETIME ";
		} else if (qdt instanceof DBPoint2D) {
			return " VARCHAR(2000) ";
		} else if (qdt instanceof DBLine2D) {
			return " VARCHAR(2001) ";
		} else {
			return super.getSQLTypeOfDBDatatype(qdt);
		}
	}

	@Override
	public void sanityCheckDBTableField(DBTableField dbTableField) {
		if (dbTableField.isPrimaryKey && dbTableField.columnType.equals(DBInteger.class)) {
			dbTableField.isAutoIncrement = true;
		}
	}

	@Override
	public boolean prefersLargeObjectsReadAsBase64CharacterStream() {
		return true;
	}

	@Override
	public boolean prefersLargeObjectsSetAsBase64String() {
		return true;
	}

	@Override
	public String doSubstringTransform(String originalString, String start, String length) {
		return " SUBSTR("
				+ originalString
				+ ", "
				+ start
				+ ","
				+ length
				+ ") ";
	}

	@Override
	protected String getCurrentDateOnlyFunctionName() {
		return "DATETIME";
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return " strftime('%Y-%m-%d %H:%M:%f', 'now','localtime') ";
	}

	@Override
	protected String getCurrentDateTimeFunction() {
		return " strftime('%Y-%m-%d %H:%M:%f', 'now','localtime') ";
	}

	@Override
	public String getStringLengthFunctionName() {
		return "LENGTH";
	}

	@Override
	public String getTruncFunctionName() {
		// TRUNC is defined in SQLiteDB as a user defined function.
		return "TRUNC";
	}

	@Override
	public String doPositionInStringTransform(String originalString, String stringToFind) {
		return "LOCATION_OF(" + originalString + ", " + stringToFind + ")";
	}

	@Override
	public String getCurrentUserFunctionName() {
		return "CURRENT_USER()";
	}

	@Override
	public String getStandardDeviationFunctionName() {
		return "STDEV";
	}

	@Override
	public String doMonthTransform(String dateExpression) {
		return " (CAST(strftime('%m', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doYearTransform(String dateExpression) {
		return " (CAST(strftime('%Y', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doDayTransform(String dateExpression) {
		return " (CAST(strftime('%d', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doHourTransform(String dateExpression) {
		return " (CAST(strftime('%H', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doMinuteTransform(String dateExpression) {
		return " (CAST(strftime('%M', " + dateExpression + ") as INTEGER))";
	}

	@Override
	public String doSecondTransform(String dateExpression) {
		return " (CAST(strftime('%S', " + dateExpression + ") as INTEGER))";
	}

//	@Override
//	public String doMillisecondTransform(String dateExpression) {
//		return " ((CAST(strftime('%f', " + dateExpression + ") as REAL)*1000)-(CAST(strftime('%S', " + dateExpression + ") as INTEGER)*1000))";
//	}
	@Override
	public String getGreatestOfFunctionName() {
		return " MAX "; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String getLeastOfFunctionName() {
		return " MIN "; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean prefersDatesReadAsStrings() {
		return true;
	}

	@Override
	public Date parseDateFromGetString(String getStringDate) throws ParseException {
		return DATETIME_FORMAT.parse(getStringDate);
	}

	public String formatDateForGetString(Date date) throws ParseException {
		return DATETIME_FORMAT.format(date);
	}

	@Override
	public boolean supportsRetrievingLastInsertedRowViaSQL() {
		return true;
	}

	@Override
	public String getRetrieveLastInsertedRowSQL() {
		return "select last_insert_rowid();";
	}

	/**
	 * Indicates whether the database supports the modulus function.
	 *
	 * @return the default implementation returns TRUE.
	 */
	@Override
	public boolean supportsModulusFunction() {
		return false;
	}

//	@Override
//	public String doAddMillisecondsTransform(String dateValue, String numberOfMilliseconds) {
//		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (" + numberOfMilliseconds + "/1000.0)||' SECOND' )";
//	}
	@Override
	public String doAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (" + numberOfSeconds + ")||' SECOND')";
	}

	@Override
	public String doAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (" + numberOfMinutes + ")||' minute')";
	}

	@Override
	public String doAddHoursTransform(String dateValue, String numberOfHours) {
		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (" + numberOfHours + ")||' hour')";
	}

	@Override
	public String doAddDaysTransform(String dateValue, String numberOfDays) {
		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (" + numberOfDays + ")||' days')";
	}

	@Override
	public String doAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (7*(" + numberOfWeeks + "))||' days')";
	}

	@Override
	public String doAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (" + numberOfMonths + ")||' month')";
	}

	@Override
	public String doAddYearsTransform(String dateValue, String numberOfYears) {
		return "strftime('%Y-%m-%d %H:%M:%f', (" + dateValue + "), (" + numberOfYears + ")||' year')";
	}

	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return "(julianday(" + otherDateValue + ") - julianday(" + dateValue + "))";
	}

	@Override
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return "(" + doDayDifferenceTransform(dateValue, otherDateValue) + "/7)";
	}

	@Override
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return "(strftime('%m'," + otherDateValue + ")+12*strftime('%Y'," + otherDateValue + ")) - (strftime('%m'," + dateValue + ")+12*strftime('%Y'," + dateValue + "))";
	}

	@Override
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return "(strftime('%Y'," + otherDateValue + ")) - (strftime('%Y'," + dateValue + "))";
	}

	@Override
	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return "(cast((strftime('%s'," + otherDateValue + ")-strftime('%s'," + dateValue + ")) AS real)/60/60)";
	}

	@Override
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "(cast((strftime('%s'," + otherDateValue + ")-strftime('%s'," + dateValue + ")) AS real)/60)";
	}

	@Override
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "cast((strftime('%s'," + otherDateValue + ")-strftime('%s'," + dateValue + ")) AS real)";
	}

//	@Override
//	public String doMillisecondDifferenceTransform(String dateValue, String otherDateValue) {
//		return "((CAST(strftime('%f',"+dateValue+") AS real)*1000.0)-(CAST(strftime('%s',("+otherDateValue+")) as INTEGER)*1000.0))"; 
//	}
	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " (cast(STRFTIME('%w', (" + dateSQL + ")) AS real)+1)";
	}

	@Override
	public boolean supportsArraysNatively() {
		return false;
	}

	@Override
	public String getAlterTableAddForeignKeyStatement(DBRow newTableRow, PropertyWrapper field) {
		if (field.isForeignKey()) {
			return "ALTER TABLE " + this.formatTableName(newTableRow) + " ADD " + field.columnName() + " REFERENCES " + field.referencedTableName() + "(" + field.referencedColumnName() + ") ";

		}
		return "";
	}

	@Override
	public String transformPeriodIntoDateRepeat(Period interval) {
		return "'" + DateRepeatImpl.getDateRepeatString(interval) + "'";
	}

	@Override
	public String doDateMinusToDateRepeatTransformation(String leftHandSide, String rightHandSide) {
		return " " + DateRepeatFunctions.DATEREPEAT_CREATION_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDatePlusDateRepeatTransform(String leftHandSide, String rightHandSide) {
		return " " + DateRepeatFunctions.DATEREPEAT_DATEADDITION_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateMinusDateRepeatTransform(String leftHandSide, String rightHandSide) {
		return " " + DateRepeatFunctions.DATEREPEAT_DATESUBTRACTION_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatEqualsTransform(String leftHandSide, String rightHandSide) {
		return "(" + leftHandSide + " = " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatLessThanTransform(String leftHandSide, String rightHandSide) {
		return DateRepeatFunctions.DATEREPEAT_LESSTHAN_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatLessThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return DateRepeatFunctions.DATEREPEAT_LESSTHANEQUALS_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatGreaterThanTransform(String leftHandSide, String rightHandSide) {
		return DateRepeatFunctions.DATEREPEAT_GREATERTHAN_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatGreaterThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return DateRepeatFunctions.DATEREPEAT_GREATERTHANEQUALS_FUNCTION + "(" + leftHandSide + ", " + rightHandSide + ")";
	}

	@Override
	public String doDateRepeatGetYearsTransform(String intervalStr) {
		return DateRepeatFunctions.DATEREPEAT_YEAR_PART_FUNCTION + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetMonthsTransform(String intervalStr) {
		return DateRepeatFunctions.DATEREPEAT_MONTH_PART_FUNCTION + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetDaysTransform(String intervalStr) {
		return DateRepeatFunctions.DATEREPEAT_DAY_PART_FUNCTION + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetHoursTransform(String intervalStr) {
		return DateRepeatFunctions.DATEREPEAT_HOUR_PART_FUNCTION + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetMinutesTransform(String intervalStr) {
		return DateRepeatFunctions.DATEREPEAT_MINUTE_PART_FUNCTION + "(" + intervalStr + ")";
	}

	@Override
	public String doDateRepeatGetSecondsTransform(String intervalStr) {
		return DateRepeatFunctions.DATEREPEAT_SECOND_PART_FUNCTION + "(" + intervalStr + ")";
	}

	@Override
	public boolean supportsArcSineFunction() {
		return false;
	}

	@Override
	public String transformCoordinatesIntoDatabasePointFormat(String xValue, String yValue) {
		return "'POINT (" + xValue + " " + yValue + ")'";
	}

	@Override
	public String doPoint2DEqualsTransform(String firstPoint, String secondPoint) {
		return Point2DFunctions.EQUALS_FUNCTION + "(" + firstPoint + ", " + secondPoint + ")";
	}

	@Override
	public String doPoint2DGetXTransform(String toSQLString) {
		return Point2DFunctions.GETX_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doPoint2DGetYTransform(String toSQLString) {
		return Point2DFunctions.GETY_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doPoint2DDimensionTransform(String toSQLString) {
		return Point2DFunctions.GETDIMENSION_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doPoint2DGetBoundingBoxTransform(String toSQLString) {
		return Point2DFunctions.GETBOUNDINGBOX_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doPoint2DAsTextTransform(String toSQLString) {
		return Point2DFunctions.ASTEXT_FUNCTION + "(" + toSQLString + ")";
	}

	@Override
	public String doDBPolygon2DFormatTransform(Polygon geom) {
		String wktValue = geom.toText();
		return Polygon2DFunctions.CREATE_FROM_WKTPOLYGON2D+"('" + wktValue + "')";
	}

	@Override
	public String doPolygon2DGetMaxXTransform(String toSQLString) {
		return Polygon2DFunctions.MAX_X + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetMinXTransform(String toSQLString) {
		return Polygon2DFunctions.MIN_X + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetMaxYTransform(String toSQLString) {
		return Polygon2DFunctions.MAX_Y + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetMinYTransform(String toSQLString) {
		return Polygon2DFunctions.MIN_Y + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetDimensionTransform(String toSQLString) {
		return Polygon2DFunctions.DIMENSION + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetBoundingBoxTransform(String toSQLString) {
		return Polygon2DFunctions.BOUNDINGBOX + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DGetAreaTransform(String polygonSQL) {
		return Polygon2DFunctions.AREA + "(" + polygonSQL+")";
	}

	@Override
	public String doPolygon2DGetExteriorRingTransform(String firstGeometry) {
		return Polygon2DFunctions.EXTERIORRING + "(" + firstGeometry+ ")";
	}

	@Override
	public String doPolygon2DEqualsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.EQUALS + "(" + firstGeometry+", "+secondGeometry + ")";
	}

	@Override
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.INTERSECTS + "(" + firstGeometry+", "+secondGeometry + ")";
	}

	@Override
	public String doPolygon2DContainsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.CONTAINS + "(" + firstGeometry+", "+secondGeometry + ")";
	}

	@Override
	public String doPolygon2DDoesNotIntersectTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.DISJOINT + "(" + firstGeometry+", "+secondGeometry + ")";
	}

	@Override
	public String doPolygon2DOverlapsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.OVERLAPS + "(" + firstGeometry+", "+secondGeometry + ")";
	}

	@Override
	public String doPolygon2DTouchesTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.TOUCHES + "(" + firstGeometry+", "+secondGeometry + ")";
	}

	@Override
	public String doPolygon2DWithinTransform(String firstGeometry, String secondGeometry) {
		//indicate whether g1 is spatially within g2. This is the inverse of Contains(). 
		// i.e. G1.within(G2) === G2.contains(G1)
		return Polygon2DFunctions.WITHIN + "(" + firstGeometry+", "+secondGeometry + ")";
	}

	@Override
	public String doLine2DAsTextTransform(String lineSQL) {
		return Line2DFunctions.ASTEXT_FUNCTION + "(" + lineSQL + ")";
	}

	@Override
	public String doLine2DEqualsTransform(String firstLine, String secondLine) {
		return Line2DFunctions.EQUALS_FUNCTION + "(" + firstLine + ", " + secondLine + ")";
	}

	@Override
	public String doLine2DDimensionTransform(String lineSQL) {
		return Line2DFunctions.GETDIMENSION_FUNCTION + "(" + lineSQL + ")";
	}

	@Override
	public String doLine2DGetBoundingBoxTransform(String lineSQL) {
		return Line2DFunctions.GETBOUNDINGBOX_FUNCTION + "(" + lineSQL + ")";
	}

	@Override
	public String doLine2DGetMaxXTransform(String lineSQL) {
		return Line2DFunctions.GETMAXX_FUNCTION + "(" + lineSQL + ")";
	}

	@Override
	public String doLine2DGetMaxYTransform(String lineSQL) {
		return Line2DFunctions.GETMAXY_FUNCTION + "(" + lineSQL + ")";
	}

	@Override
	public String doLine2DGetMinXTransform(String lineSQL) {
		return Line2DFunctions.GETMINX_FUNCTION + "(" + lineSQL + ")";
	}

	@Override
	public String doLine2DGetMinYTransform(String lineSQL) {
		return Line2DFunctions.GETMINY_FUNCTION + "(" + lineSQL + ")";
	}
}
