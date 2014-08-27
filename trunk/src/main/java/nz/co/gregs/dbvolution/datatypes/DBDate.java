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
package nz.co.gregs.dbvolution.datatypes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.DateResult;
import nz.co.gregs.dbvolution.operators.DBLikeCaseInsensitiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeExclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeInclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesOperator;

/**
 * Encapsulates database values that are Dates.
 *
 * <p>
 * Use DBDate when the column is a date datatype, even in databases where the
 * native date type is a String (i.e. {@link SQLiteDB}).
 *
 * <p>
 * Generally DBDate is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBDate myBoolColumn = new DBDate();}
 *
 * @author Gregory Graham
 */
public class DBDate extends QueryableDatatype implements DateResult {

	private static final long serialVersionUID = 1L;

	/**
	 * The default constructor for DBDate.
	 *
	 * <p>
	 * Creates an unset undefined DBDate object.
	 *
	 */
	public DBDate() {
		super();
	}

	/**
	 * Creates a DBDate with the value provided.
	 *
	 * <p>
	 * The resulting DBDate will be set as having the value provided but will not
	 * be defined in the database.
	 *
	 * @param date
	 */
	public DBDate(Date date) {
		super(date);
	}

	/**
	 * Creates a column expression with a date result from the expression
	 * provided.
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param dateExpression
	 */
	public DBDate(DateResult dateExpression) {
		super(dateExpression);
	}

	/**
	 * Creates a DBDate with the value provided.
	 *
	 * <p>
	 * The resulting DBDate will be set as having the value provided but will not
	 * be defined in the database.
	 *
	 * @param timestamp
	 */
	DBDate(Timestamp timestamp) {
		super(timestamp);
		if (timestamp == null) {
			this.isDBNull = true;
		} else {
			Date date = new Date();
			date.setTime(timestamp.getTime());
			literalValue = date;
		}
	}

	/**
	 * Creates a DBDate with the value provided.
	 *
	 * <p>
	 * The resulting DBDate will be set as having the value provided but will not
	 * be defined in the database.
	 *
	 * <p>
	 * The string is parsed using {@link Date#parse(java.lang.String) } so please
	 * ensure your string matches the requirements of that method.
	 *
	 * @param dateAsAString
	 */
	@SuppressWarnings("deprecation")
	DBDate(String dateAsAString) {
		final long dateLong = Date.parse(dateAsAString);
		Date dateValue = new Date();
		dateValue.setTime(dateLong);
		literalValue = dateValue;
	}

	@Override
	public String getWhereClause(DBDatabase db, String columnName) {
		if (this.getOperator() instanceof DBLikeCaseInsensitiveOperator) {
			throw new RuntimeException("DATE COLUMNS CAN'T USE \"LIKE\": " + columnName);
		} else {
			return super.getWhereClause(db, columnName);
		}
	}

	/**
	 * Returns the set value of this DBDate as a Java Date instance.
	 *
	 * @return the value as a Java Date.
	 */
	public Date dateValue() {
		if (literalValue instanceof Date) {
			return (Date) literalValue;
		} else {
			return null;
		}
	}

	@Override
	public void setValue(Object newLiteralValue) {
		if (newLiteralValue instanceof Date) {
			setValue((Date) newLiteralValue);
		} else if (newLiteralValue instanceof DBDate) {
			setValue(((QueryableDatatype) newLiteralValue).literalValue);
		} else {
			throw new ClassCastException(this.getClass().getSimpleName() + ".setValue() Called With A Non-Date: Use only Dates with this class");
		}
	}

	/**
	 * Sets the value of this QDT to the Java Date provided.
	 *
	 * @param date
	 */
	public void setValue(Date date) {
		super.setLiteralValue(date);
	}

	/**
	 * Sets the value of this QDT to the dateStr provided.
	 *
	 * <p>
	 * The date String will be parsed by {@link Date#parse(java.lang.String) } so
	 * please confirms to the requirements of that method.
	 *
	 * @param dateStr
	 */
	@SuppressWarnings("deprecation")
	public void setValue(String dateStr) {
		final long dateLong = Date.parse(dateStr);
		Date date = new Date();
		date.setTime(dateLong);
		setValue(date);
	}

	@Override
	public String getSQLDatatype() {
		return "TIMESTAMP";
	}

	/**
	 * Returns the string value of the DBDate.
	 *
	 * @return a string version of the current value of this DBDate
	 */
	@Override
	public String toString() {
		if (this.isDBNull || dateValue() == null) {
			return "";
		}
		return dateValue().toString();
	}

	@Override
	public String formatValueForSQLStatement(DBDatabase db) {
		return db.getDefinition().getDateFormattedForQuery(dateValue());
	}

	@Override
	public void setFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException {
		blankQuery();
		if (resultSet == null || fullColumnName == null) {
			this.setToNull();
		} else {
			java.sql.Date dbValue;
			if (database.getDefinition().prefersDatesReadAsStrings()) {
				dbValue = setByGetString(database, resultSet, fullColumnName);
			} else {
				dbValue = setByGetDate(database, resultSet, fullColumnName);
			}
			if (dbValue == null) {
				this.setToNull();
			} else {
				this.setValue(dbValue);
			}
		}
		setUnchanged();
		setDefined(true);
	}

	private java.sql.Date setByGetString(DBDatabase database, ResultSet resultSet, String fullColumnName) {
		String string = null;
		try {
			string = resultSet.getString(fullColumnName);
		} catch (SQLException sqlex) {
			sqlex.printStackTrace();
		}
		if (string == null || string.isEmpty()) {
			return null;
		} else {
			try {
				return new java.sql.Date(database.getDefinition().getDateGetStringFormat().parse(string).getTime());
			} catch (ParseException ex) {
				throw new DBRuntimeException("Unable To Parse Date: " + string, ex);
			}
		}
	}

	private java.sql.Date setByGetDate(DBDatabase database, ResultSet resultSet, String fullColumnName) {
		java.sql.Date dbValue = null;
		try {
			java.sql.Date dateValue = resultSet.getDate(fullColumnName);
			if (resultSet.wasNull()) {
				dbValue = null;
			} else {
				// Some drivers interpret getDate as meaning return only the date without the time
				// so we should check both the date and the timestamp find the latest time.
				final long timestamp = resultSet.getTimestamp(fullColumnName).getTime();
				java.sql.Date timestampValue = new java.sql.Date(timestamp);
				if (timestampValue.after(dateValue)) {
					dbValue = timestampValue;
				} else {
					dbValue = dateValue;
				}
			}
		} catch (SQLException sqlex) {
			sqlex.printStackTrace();
		}
		return dbValue;
	}

	@Override
	public DBDate copy() {
		return (DBDate) super.copy(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Date getValue() {
		return dateValue();
	}

	@Override
	public DBDate getQueryableDatatypeForExpressionValue() {
		return new DBDate();
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return new HashSet<DBRow>();
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted
	 */
	public void permittedValues(Date... permitted) {
		this.setOperator(new DBPermittedValuesOperator((Object[]) permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded
	 */
	public void excludedValues(Date... excluded) {
		this.setOperator(new DBPermittedValuesOperator((Object[]) excluded));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound
	 * @param upperBound
	 */
	public void permittedRange(Date lowerBound, Date upperBound) {
		setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended downwards and
	 * inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound
	 * @param upperBound
	 */
	public void permittedRangeInclusive(Date lowerBound, Date upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
	 * return 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5,...
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1,...
	 *
	 * @param lowerBound
	 * @param upperBound
	 */
	public void permittedRangeExclusive(Date lowerBound, Date upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
	}
	
	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e excludedRange(1,3) will
	 * return everything except 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e excludedRange(1,null) will return ..., -1, 0.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e excludedRange(null, 5) will return 5, 6, 7, 8 etc.
	 *
	 * @param lowerBound
	 * @param upperBound
	 */
	public void excludedRange(Date lowerBound, Date upperBound) {
		setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e excludedRangeInclusive(1,3) will
	 * return ..., -1, 0, 4, 5, ... .
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(1,null) will return ..., -1, 0.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended downwards and
	 * inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(null, 5) will return 6, 7, 8, 9,... etc.
	 *
	 * @param lowerBound
	 * @param upperBound
	 */
	public void excludedRangeInclusive(Date lowerBound, Date upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e excludedRangeExclusive(1,3) will
	 * return ... -1, 0, 1, 3, 4,... but exclude 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(1,null) will return ..., -1 ,0 ,1 .
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(null, 5) will return 5,6,7,8...
	 *
	 * @param lowerBound
	 * @param upperBound
	 */
	public void excludedRangeExclusive(Date lowerBound, Date upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted
	 */
	public void permittedValues(DateExpression... permitted) {
		this.setOperator(new DBPermittedValuesOperator((Object[]) permitted));
	}

	/**
	 *
	 * excludes the object, Set, List, Array, or vararg of objects
	 *
	 *
	 * @param excluded
	 */
	public void excludedValues(DateExpression... excluded) {
		this.setOperator(new DBPermittedValuesOperator((Object[]) excluded));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e permittedRange(1,3) will
	 * return 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound
	 * @param upperBound
	 */
	public void permittedRange(DateExpression lowerBound, DateExpression upperBound) {
		setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e permittedRangeInclusive(1,3) will
	 * return 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * inclusive.
	 * <br>
	 * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
	 *
	 * @param lowerBound
	 * @param upperBound
	 */
	public void permittedRangeInclusive(DateExpression lowerBound, DateExpression upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
	 * return 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
	 *
	 * @param lowerBound
	 * @param upperBound
	 */
	public void permittedRangeExclusive(DateExpression lowerBound, DateExpression upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
	}
	
	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified the lower-bound will be included in
	 * the search and the upper-bound excluded. I.e excludedRange(1,3) will
	 * return everything except 1 and 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e excludedRange(1,null) will return 0, -1, -2, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e excludedRange(null, 5) will return 5, 6, 7, 8, etc.
	 *
	 * @param lowerBound
	 * @param upperBound
	 */
	public void excludedRange(DateExpression lowerBound, DateExpression upperBound) {
		setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be included in the search. I.e excludedRangeInclusive(1,3) will
	 * return everything except 1, 2, and 3.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(1,null) will return ..., -1, 0.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * inclusive.
	 * <br>
	 * I.e excludedRangeInclusive(null, 5) will return 6, 7, 8, 9, etc.
	 *
	 * @param lowerBound
	 * @param upperBound
	 */
	public void excludedRangeInclusive(DateExpression lowerBound, DateExpression upperBound) {
		setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Performs searches based on a range.
	 *
	 * if both ends of the range are specified both the lower- and upper-bound
	 * will be excluded in the search. I.e excludedRangeExclusive(1,3) will
	 * return everything except 2.
	 *
	 * <p>
	 * if the upper-bound is null the range will be open ended upwards and
	 * exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(1,null) will return 0, -1, -2, etc.
	 *
	 * <p>
	 * if the lower-bound is null the range will be open ended downwards and
	 * exclusive.
	 * <br>
	 * I.e excludedRangeExclusive(null, 5) will return 5, 6, 7, 8,, etc.
	 *
	 * @param lowerBound
	 * @param upperBound
	 */
	public void excludedRangeExclusive(DateExpression lowerBound, DateExpression upperBound) {
		setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
		negateOperator();
	}

	/**
	 * Used internally to decide whether the required query needs to include NULL values.
	 *
	 * @return whether the query expression needs to test for NULL.
	 */
	@Override
	public boolean getIncludesNull() {
		return dateValue() == null;
	}
}
