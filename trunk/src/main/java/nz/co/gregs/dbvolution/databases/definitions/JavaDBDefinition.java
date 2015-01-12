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
package nz.co.gregs.dbvolution.databases.definitions;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBJavaObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.query.QueryOptions;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * The DBDefinition to use for JavaDB databases.
 *
 * @author Gregory Graham
 */
public class JavaDBDefinition extends DBDefinition {

	private static final DateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final String[] reservedWordsArray = new String[]{};
	private static final List<String> reservedWords = Arrays.asList(reservedWordsArray);

	@Override
	public String getDateFormattedForQuery(Date date) {
//		yyyy-mm-dd hh[:mm[:ss
		return "TIMESTAMP('" + DATETIME_FORMAT.format(date) + "')";
	}

	@Override
	protected String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBBoolean) {
			return "SMALLINT";
		} else if (qdt instanceof DBJavaObject) {
			return "BLOB";
		} else if (qdt instanceof DBDate) {
			return "TIMESTAMP";
		} else {
			return super.getSQLTypeOfDBDatatype(qdt); //To change body of generated methods, choose Tools | Templates.
		}
	}

	@Override
	public String formatTableName(DBRow table
	) {
		final String sqlObjectName = table.getTableName();
		return formatNameForJavaDB(sqlObjectName);
	}

	@Override
	public String getPrimaryKeySequenceName(String table, String column
	) {
		return formatNameForJavaDB(super.getPrimaryKeySequenceName(table, column));
	}

	@Override
	public String getPrimaryKeyTriggerName(String table, String column
	) {
		return formatNameForJavaDB(super.getPrimaryKeyTriggerName(table, column));
	}

	@Override
	public String formatColumnName(String column
	) {
		return formatNameForJavaDB(super.formatColumnName(column));
	}

	private static String formatNameForJavaDB(final String sqlObjectName) {
		if (sqlObjectName.length() < 30 && !(reservedWords.contains(sqlObjectName.toUpperCase()))) {
			return sqlObjectName.replaceAll("^[_-]", "O").replaceAll("-", "_");
		} else {
			return ("O" + sqlObjectName.hashCode()).replaceAll("^[_-]", "O").replaceAll("-", "_");
		}
	}

	@Override
	public String formatTableAlias(String tabRow) {
		return "\"" + tabRow.replaceAll("-","_") + "\"";
	}

	@Override
	public String formatForColumnAlias(final String actualName) {
		String formattedName = actualName.replaceAll("\\.", "__");
		return ("DB" + formattedName.hashCode()).replaceAll("-", "_") + "";
	}

	@Override
	public String beginTableAlias() {
		return " ";
	}

	@Override
	public String endInsertLine() {
		return "";
	}

	@Override
	public String endDeleteLine() {
		return "";
	}

	@Override
	public String endSQLStatement() {
		return "";
	}

	@Override
	public String getStringLengthFunctionName() {
		return "LENGTH";
	}

	@Override
	public String doSubstringTransform(String originalString, String start, String length) {
		return " SUBSTR("
				+ originalString
				+ ", "
				+ start
				+ (length.trim().isEmpty() ? "" : ", " + length)
				+ ") ";
	}

	@Override
	public boolean prefersLargeObjectsReadAsBLOB() {
		return true;
	}

	@Override
	public boolean prefersLargeObjectsSetAsBLOB() {
		return false;
	}

	@Override
	public boolean supportsPagingNatively(QueryOptions options) {
		return false;
	}

	@Override
	public String doTruncTransform(String realNumberExpression, String numberOfDecimalPlacesExpression) {
		return "(case when " + realNumberExpression + " >= 0 then floor(exp(" + numberOfDecimalPlacesExpression + " * ln(10)) * " + realNumberExpression + ") / exp(" + numberOfDecimalPlacesExpression + " * ln(10)) else ceil(exp(" + numberOfDecimalPlacesExpression + " * ln(10)) * " + realNumberExpression + ") / exp(" + numberOfDecimalPlacesExpression + " * ln(10)) end)";
	}

	@Override
	public String doModulusTransform(String firstNumber, String secondNumber) {
		return " MOD(" + firstNumber + "," + secondNumber + ") ";
	}

	@Override
	protected boolean supportsGreatestOfNatively() {
		return false;
	}

	@Override
	protected boolean supportsLeastOfNatively() {
		return false;
	}

	@Override
	public String doPositionInStringTransform(String originalString, String stringToFind) {
		return "LOCATE(" + stringToFind + ", " + originalString + ")";
	}

	@Override
	public String doYearTransform(String dateExpression) {
		return "YEAR(" + dateExpression + ")";
	}

	@Override
	public String doMonthTransform(String dateExpression) {
		return "MONTH(" + dateExpression + ")";
	}

	@Override
	public String doDayTransform(String dateExpression) {
		return "DAY(" + dateExpression + ")";
	}

	@Override
	public String doHourTransform(String dateExpression) {
		return "HOUR(" + dateExpression + ")";
	}

	@Override
	public String doMinuteTransform(String dateExpression) {
		return "MINUTE(" + dateExpression + ")";
	}

	@Override
	public String doSecondTransform(String dateExpression) {
		return "SECOND(" + dateExpression + ")";
	}

	@Override
	public String doAddSecondsTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_SECOND, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doAddMinutesTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_MINUTE, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doAddHoursTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_HOUR, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doAddDaysTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_DAY, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doAddWeeksTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_WEEK, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doAddMonthsTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_MONTH, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doAddYearsTransform(String dateExpression, String numberOfDays) {
		return "cast({fn timestampadd(SQL_TSI_YEAR, " + numberOfDays + ", " + dateExpression + ")} as timestamp)";
	}

	@Override
	public String doReplaceTransform(String withinString, String findString, String replaceString) {
		String startIndex = "Locate(" + findString + "," + withinString + ")";
		String length = "Length(" + findString + ")";
		return "(case when " + startIndex + "> 0 then SUBSTR(" + withinString + ", 1, " + startIndex + "-1)||" + replaceString + "||SUBSTR(" + withinString + ", " + startIndex + "+" + length + ") else " + withinString + " end)";
	}

	@Override
	public String doStringEqualsTransform(String firstSQLExpression, String secondSQLExpression) {
		return "(" + super.doStringEqualsTransform(firstSQLExpression, secondSQLExpression) + " AND LENGTH(" + firstSQLExpression + ") = LENGTH(" + secondSQLExpression + "))";
	}

	@Override
	public String doNumberToStringTransform(String numberExpression) {
		return "trim(cast(cast(" + numberExpression + " as char(38)) as varchar(1000)))";
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return "cast(cast((cast( " + getCurrentDateOnlyFunctionName() + "  as VARCHAR(1000))||' 00:00:00') as VARCHAR(1000)) as TIMESTAMP) ";
	}

	@Override
	public boolean supportsStandardDeviationFunction() {
		return false;
	}

	@Override
	public Object getOrderByDirectionClause(Boolean sortOrder) {
		return super.getOrderByDirectionClause(sortOrder) + (sortOrder ? " NULLS FIRST " : " NULLS LAST ");
	}

//	@Override
//	public String endOrderByClause() {
//		return " NULLS FIRST ";
//	}
//	@Override
//	public String beginStringValue() {
//		return "cast('";
//	}
//
//	@Override
//	public String endStringValue() {
//		return "' as VARCHAR(1000))"; //To change body of generated methods, choose Tools | Templates.
//	}
}
