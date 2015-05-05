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
package nz.co.gregs.dbvolution.internal.sqlite;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.databases.definitions.SQLiteDefinition;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.internal.datatypes.DateRepeatImpl;
import org.sqlite.Function;

/**
 *
 * @author gregorygraham
 */
public class DateRepeatFunctions {

	public static String DATEREPEAT_GREATERTHANEQUALS_FUNCTION = "DBV_DATEREPEAT_GREATERTHANEQUALS";
	public static String DATEREPEAT_MINUTE_PART_FUNCTION = "DBV_DATEREPEAT_MINUTE_PART";
	public static String DATEREPEAT_YEAR_PART_FUNCTION = "DBV_DATEREPEAT_YEAR_PART";
	public static String DATEREPEAT_CREATION_FUNCTION = "DBV_DATEREPEAT_CREATE";
	public static String DATEREPEAT_DATEADDITION_FUNCTION = "DBV_DATEREPEAT_DATEADD";
	public static String DATEREPEAT_DATESUBTRACTION_FUNCTION = "DBV_DATEREPEAT_DATEMINUS";
	public static String DATEREPEAT_DAY_PART_FUNCTION = "DBV_DATEREPEAT_DAY_PART";
	public static String DATEREPEAT_HOUR_PART_FUNCTION = "DBV_DATEREPEAT_HOUR_PART";
	public static String DATEREPEAT_SECOND_PART_FUNCTION = "DBV_DATEREPEAT_SECOND_PART";
	public static String DATEREPEAT_GREATERTHAN_FUNCTION = "DBV_DATEREPEAT_GREATERTHAN";
	public static String DATEREPEAT_MONTH_PART_FUNCTION = "DBV_DATEREPEAT_MONTH_PART";
	public static String DATEREPEAT_EQUALS_FUNCTION = "DBV_DATEREPEAT_EQUALS";
	public static String DATEREPEAT_LESSTHANEQUALS_FUNCTION = "DBV_DATEREPEAT_LESSTHANEQUALS";
	public static String DATEREPEAT_LESSTHAN_FUNCTION = "DBV_DATEREPEAT_LESSTHAN";

	private DateRepeatFunctions() {
	}

	public static void addFunctions(java.sql.Connection connection) throws SQLException {
		Function.create(connection, DATEREPEAT_CREATION_FUNCTION, new DateRepeatFunctions.Create());
		Function.create(connection, DATEREPEAT_EQUALS_FUNCTION, new DateRepeatFunctions.Equals());
		Function.create(connection, DATEREPEAT_LESSTHAN_FUNCTION, new DateRepeatFunctions.LessThan());
		Function.create(connection, DATEREPEAT_GREATERTHAN_FUNCTION, new DateRepeatFunctions.GreaterThan());
		Function.create(connection, DATEREPEAT_LESSTHANEQUALS_FUNCTION, new DateRepeatFunctions.LessThanOrEqual());
		Function.create(connection, DATEREPEAT_GREATERTHANEQUALS_FUNCTION, new DateRepeatFunctions.GreaterThanOrEqual());
		Function.create(connection, DATEREPEAT_DATEADDITION_FUNCTION, new DateRepeatFunctions.DateAddition());
		Function.create(connection, DATEREPEAT_DATESUBTRACTION_FUNCTION, new DateRepeatFunctions.DateSubtraction());
		Function.create(connection, DATEREPEAT_YEAR_PART_FUNCTION, new DateRepeatFunctions.GetYear());
		Function.create(connection, DATEREPEAT_MONTH_PART_FUNCTION, new DateRepeatFunctions.GetMonth());
		Function.create(connection, DATEREPEAT_DAY_PART_FUNCTION, new DateRepeatFunctions.GetDay());
		Function.create(connection, DATEREPEAT_HOUR_PART_FUNCTION, new DateRepeatFunctions.GetHour());
		Function.create(connection, DATEREPEAT_MINUTE_PART_FUNCTION, new DateRepeatFunctions.GetMinute());
		Function.create(connection, DATEREPEAT_SECOND_PART_FUNCTION, new DateRepeatFunctions.GetSecond());
	}

	public static String formatDateForGetString(Date date) throws ParseException {
		return SQLiteDefinition.DATETIME_FORMAT.format(date);
	}

	public static class Create extends Function {

		@Override
		protected void xFunc() throws SQLException {
			try {
				SQLiteDefinition defn = new SQLiteDefinition();
				final String originalStr = value_text(0);
				final String compareToStr = value_text(1);
				if (originalStr == null || compareToStr == null) {
					result((String) null);
				} else {
					Date original = defn.parseDateFromGetString(originalStr);
					Date compareTo = defn.parseDateFromGetString(compareToStr);
					String intervalString = DateRepeatImpl.repeatFromTwoDates(original, compareTo);
					result(intervalString);
				}
			} catch (ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new RuntimeException("Failed To Parse SQLite Date", ex);
			}
		}
	}

	public static class DateAddition extends Function {

		public DateAddition() {
		}

		@Override
		protected void xFunc() throws SQLException {
			try {
				SQLiteDefinition defn = new SQLiteDefinition();
				final String dateStr = value_text(0);
				final String intervalStr = value_text(1);
				if (dateStr == null || intervalStr == null || intervalStr.length() == 0 || dateStr.length() == 0) {
					result((String) null);
				} else {
					Date date = defn.parseDateFromGetString(dateStr);
					Date result = DateRepeatImpl.addDateAndDateRepeatString(date, intervalStr);
					result(DateRepeatFunctions.formatDateForGetString(result));
				}
			} catch (ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new DBRuntimeException("Failed To Parse SQLite Date", ex);
			}
		}

	}

	public static class DateSubtraction extends Function {

		public DateSubtraction() {
		}

		@Override
		protected void xFunc() throws SQLException {
			try {
				SQLiteDefinition defn = new SQLiteDefinition();
				final String dateStr = value_text(0);
				final String intervalStr = value_text(1);
				if (dateStr == null || intervalStr == null || dateStr.length() == 0 || intervalStr.length() == 0) {
					result((String) null);
				} else {
					Date date = defn.parseDateFromGetString(dateStr);
					Date result = DateRepeatImpl.subtractDateAndDateRepeatString(date, intervalStr);
					result(DateRepeatFunctions.formatDateForGetString(result));
				}
			} catch (ParseException ex) {
				Logger.getLogger(SQLiteDB.class.getName()).log(Level.SEVERE, null, ex);
				throw new DBRuntimeException("Failed To Parse SQLIte Date", ex);
			}
		}

	}

	public static class Equals extends Function {

		@Override
		protected void xFunc() throws SQLException {
			final String originalStr = value_text(0);
			final String compareToStr = value_text(1);
			if (originalStr == null || compareToStr == null) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.compareDateRepeatStrings(originalStr, compareToStr);
				result(result == 0 ? 1 : 0);
			}
		}

	}

	public static class GetDay extends Function {

		public GetDay() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String intervalStr = value_text(0);
			if (intervalStr == null || intervalStr.length() == 0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.getDayPart(intervalStr);
				result(result);
			}
		}

	}

	public static class GetHour extends Function {

		public GetHour() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String intervalStr = value_text(0);
			if (intervalStr == null || intervalStr.length() == 0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.getHourPart(intervalStr);
				result(result);
			}
		}

	}

	public static class GetMinute extends Function {

		public GetMinute() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String intervalStr = value_text(0);
			if (intervalStr == null || intervalStr.length() == 0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.getMinutePart(intervalStr);
				result(result);
			}
		}

	}

	public static class GetMonth extends Function {

		public GetMonth() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String intervalStr = value_text(0);
			if (intervalStr == null || intervalStr.length() == 0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.getMonthPart(intervalStr);
				result(result);
			}
		}

	}

	public static class GetSecond extends Function {

		public GetSecond() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String intervalStr = value_text(0);
			if (intervalStr == null || intervalStr.length() == 0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.getSecondPart(intervalStr);
				result(result);
			}
		}

	}

	public static class GetYear extends Function {

		public GetYear() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String intervalStr = value_text(0);
			if (intervalStr == null || intervalStr.length() == 0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.getYearPart(intervalStr);
				result(result);
			}
		}

	}

	public static class GreaterThan extends Function {

		public GreaterThan() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String originalStr = value_text(0);
			final String compareToStr = value_text(1);
			if (originalStr == null || compareToStr == null || originalStr.length() == 0 || compareToStr.length() == 0) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.compareDateRepeatStrings(originalStr, compareToStr);
				result(result == 1 ? 1 : 0);
			}
		}

	}

	public static class GreaterThanOrEqual extends Function {

		public GreaterThanOrEqual() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String originalStr = value_text(0);
			final String compareToStr = value_text(1);
			if (originalStr == null || compareToStr == null) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.compareDateRepeatStrings(originalStr, compareToStr);
				result(result > -1 ? 1 : 0);
			}
		}

	}

	public static class LessThan extends Function {

		public LessThan() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String originalStr = value_text(0);
			final String compareToStr = value_text(1);
			if (originalStr == null || compareToStr == null) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.compareDateRepeatStrings(originalStr, compareToStr);
				result(result == -1 ? 1 : 0);
			}
		}

	}

	public static class LessThanOrEqual extends Function {

		public LessThanOrEqual() {
		}

		@Override
		protected void xFunc() throws SQLException {
			final String originalStr = value_text(0);
			final String compareToStr = value_text(1);
			if (originalStr == null || compareToStr == null) {
				result((String) null);
			} else {
				int result = DateRepeatImpl.compareDateRepeatStrings(originalStr, compareToStr);
				result(result < 1 ? 1 : 0);
			}
		}

	}

}
