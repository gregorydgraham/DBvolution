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
package nz.co.gregs.dbvolution.internal.h2;

import java.sql.SQLException;
import java.sql.Statement;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;

/**
 *
 * @author gregorygraham
 */
public class DateRepeatFunctions {

	static String ACTUAL_DATATYPE = " VARCHAR(100) ";
	public static String DATATYPE = " DBV_DATEREPEAT ";

	public static String DATEREPEAT_CREATION_FUNCTION = "DBV_DATEREPEAT_CREATE";
	public static String DATEREPEAT_EQUALS_FUNCTION = "DBV_DATEREPEAT_EQUALS";
	public static String DATEREPEAT_LESSTHAN_FUNCTION = "DBV_DATEREPEAT_LESSTHAN";
	public static String DATEREPEAT_LESSTHANEQUALS_FUNCTION = "DBV_DATEREPEAT_LESSTHANEQUALS";
	public static String DATEREPEAT_GREATERTHAN_FUNCTION = "DBV_DATEREPEAT_GREATERTHAN";
	public static String DATEREPEAT_GREATERTHANEQUALS_FUNCTION = "DBV_DATEREPEAT_GREATERTHANEQUALS";
	public static String DATEREPEAT_DATEADDITION_FUNCTION = "DBV_DATEREPEAT_DATEPLUSDATEREPEAT";
	public static String DATEREPEAT_DATESUBTRACTION_FUNCTION = "DBV_DATEREPEAT_DATEMINUSDATEREPEAT";

	public static String DATEREPEAT_YEAR_PART_FUNCTION = "DBV_DATEREPEAT_YEAR_PART";
	public static String DATEREPEAT_MONTH_PART_FUNCTION = "DBV_DATEREPEAT_MONTH_PART";
	public static String DATEREPEAT_DAY_PART_FUNCTION = "DBV_DATEREPEAT_DAY_PART";
	public static String DATEREPEAT_HOUR_PART_FUNCTION = "DBV_DATEREPEAT_HOUR_PART";
	public static String DATEREPEAT_MINUTE_PART_FUNCTION = "DBV_DATEREPEAT_MINUTE_PART";
	public static String DATEREPEAT_SECOND_PART_FUNCTION = "DBV_DATEREPEAT_SECOND_PART";
//	public static String DATEREPEAT_MILLISECOND_PART_FUNCTION = "DBV_DATEREPEAT_MILLI_PART";

	private DateRepeatFunctions() {
	}

	public static void addFunctions(java.sql.Statement stmt) throws UnableToFindJDBCDriver, UnableToCreateDatabaseConnectionException, SQLException {

		addDateRepeatDatatype(stmt);
		addCreation(stmt);
		addAddition(stmt);
		addSubtract(stmt);
		addEquals(stmt);
		addGreaterThanEquals(stmt);
		addGreaterThan(stmt);
		addLessThanEquals(stmt);
		addLessThan(stmt);

		addYearPart(stmt);
		addMonthPart(stmt);
		addDayPart(stmt);
		addHourPart(stmt);
		addMinutePart(stmt);
		addSecondPart(stmt);

	}

	protected static void addDateRepeatDatatype(Statement stmt) throws SQLException {
		stmt.execute("CREATE DOMAIN IF NOT EXISTS " + DATATYPE + " AS " + ACTUAL_DATATYPE + "; ");
	}

	protected static void addCreation(Statement stmt) throws SQLException {
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + DATEREPEAT_CREATION_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "@CODE String getDateRepeatString(Date original, Date compareTo) {\n"
				+ "		if (original==null||compareTo==null){return null;}\n"
				+ "		int years = original.getYear() - compareTo.getYear();\n"
				+ "		int months = original.getMonth() - compareTo.getMonth();\n"
				+ "		int days = original.getDate() - compareTo.getDate();\n"
				+ "		int hours = original.getHours() - compareTo.getHours();\n"
				+ "		int minutes = original.getMinutes() - compareTo.getMinutes();\n"
				+ "		int millis = (int) ((original.getTime() - ((original.getTime() / 1000) * 1000)) - (compareTo.getTime() - ((compareTo.getTime() / 1000) * 1000)));\n"
				+ "		double seconds = original.getSeconds() - compareTo.getSeconds()+(millis/1000.0);\n"
				+ "		String dateRepeatString = \"P\" + years + \"Y\" + months + \"M\" + days + \"D\" + hours + \"h\" + minutes + \"n\" + seconds + \"s\";\n"
				+ "		return dateRepeatString;"
				+ "	} $$;");
	}

	protected static void addAddition(Statement stmt) throws SQLException {
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + DATEREPEAT_DATEADDITION_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "import java.lang.*;"
				+ "@CODE Date addDateAndDateRepeatString(Date original, String dateRepeatStr) {\n"
				+ "		if (original==null||dateRepeatStr==null||dateRepeatStr.length()==0||original.toString().length()==0||original.getTime()==0){return null;}else{\n"
				+ "		Calendar cal = new GregorianCalendar();\n"
				+ "		try{cal.setTime(original);}catch(Exception except){return null;}\n"
				+ "		int years = Integer.parseInt(dateRepeatStr.replaceAll(\".*P([-0-9.]+)Y.*\", \"$1\"));\n"
				+ "		int months = Integer.parseInt(dateRepeatStr.replaceAll(\".*Y([-0-9.]+)M.*\", \"$1\"));\n"
				+ "		int days = Integer.parseInt(dateRepeatStr.replaceAll(\".*M([-0-9.]+)D.*\", \"$1\"));\n"
				+ "		int hours = Integer.parseInt(dateRepeatStr.replaceAll(\".*D([-0-9.]+)h.*\", \"$1\"));\n"
				+ "		int minutes = Integer.parseInt(dateRepeatStr.replaceAll(\".*h([-0-9.]+)n.*\", \"$1\"));\n"
				+ "		int seconds = Integer.parseInt(dateRepeatStr.replaceAll(\".*m([-0-9.]+)s.*\", \"$1\"));\n"
				+ "		final Double secondsDouble = Double.parseDouble(dateRepeatStr.replaceAll(\".*n([-0-9.]+)s.*\", \"$1\"));\n"
				+ "		final int secondsInt = secondsDouble.intValue();\n"
				+ "		cal.add(Calendar.YEAR, years);\n"
				+ "		cal.add(Calendar.MONTH, months);\n"
				+ "		cal.add(Calendar.DAY_OF_MONTH, days);\n"
				+ "		cal.add(Calendar.HOUR, hours);\n"
				+ "		cal.add(Calendar.MINUTE, minutes);\n"
				+ "		cal.add(Calendar.SECOND, seconds);\n"
				+ "		return cal.getTime();}\n"
				+ "	} $$;");
	}

	protected static void addSubtract(Statement stmt) throws SQLException {
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + DATEREPEAT_DATESUBTRACTION_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "@CODE Date subtractDateAndDateRepeatString(Date original, String dateRepeatInput) {\n"
				+ "		if (original == null || dateRepeatInput == null || dateRepeatInput.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		String dateRepeatStr = dateRepeatInput.replaceAll(\"[^-.PYMDhns0-9]+\", \"\");\n"
				+ "		Calendar cal = new GregorianCalendar();\n"
				+ "		cal.setTime(original);\n"
				+ "		int years = getYearPart(dateRepeatStr);\n"
				+ "		int months = getMonthPart(dateRepeatStr);\n"
				+ "		int days = getDayPart(dateRepeatStr);\n"
				+ "		int hours = getHourPart(dateRepeatStr);\n"
				+ "		int minutes = getMinutePart(dateRepeatStr);\n"
				+ "		int seconds = getSecondPart(dateRepeatStr);\n"
				+ "\n"
				+ "		cal.add(Calendar.YEAR, -1 * years);\n"
				+ "		cal.add(Calendar.MONTH, -1 * months);\n"
				+ "		cal.add(Calendar.DAY_OF_MONTH, -1 * days);\n"
				+ "		cal.add(Calendar.HOUR, -1 * hours);\n"
				+ "		cal.add(Calendar.MINUTE, -1 * minutes);\n"
				+ "		cal.add(Calendar.SECOND, -1 * seconds);\n"
				+ "		return cal.getTime();"
				+ "	} \n"
				+ "\n"
				+ "	public static Integer getMillisecondPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr == null || dateRepeatStr.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		final Double secondsDouble = Double.parseDouble(dateRepeatStr.replaceAll(\".*n([-0-9.]+)s.*\", \"$1\"));\n"
				+ "		final int secondsInt = secondsDouble.intValue();\n"
				+ "		final int millis = (int) ((secondsDouble * 1000.0) - (secondsInt * 1000));\n"
				+ "		return millis;\n"
				+ "	}\n"
				+ "\n"
				+ "	public static Integer getSecondPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr == null || dateRepeatStr.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		return Double.valueOf(dateRepeatStr.replaceAll(\".*n([-0-9.]+)s.*\", \"$1\")).intValue();\n"
				+ "	}\n"
				+ "\n"
				+ "	public static Integer getMinutePart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr == null || dateRepeatStr.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*h([-0-9.]+)n.*\", \"$1\"));\n"
				+ "	}\n"
				+ "\n"
				+ "	public static Integer getHourPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr == null || dateRepeatStr.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*D([-0-9.]+)h.*\", \"$1\"));\n"
				+ "	}\n"
				+ "\n"
				+ "	public static Integer getDayPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr == null || dateRepeatStr.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*M([-0-9.]+)D.*\", \"$1\"));\n"
				+ "	}\n"
				+ "\n"
				+ "	public static Integer getMonthPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr == null || dateRepeatStr.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*Y([-0-9.]+)M.*\", \"$1\"));\n"
				+ "	}\n"
				+ "\n"
				+ "	public static Integer getYearPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr == null || dateRepeatStr.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*P([-0-9.]+)Y.*\", \"$1\"));\n"
				+ "	}$$;");
	}

	protected static void addEquals(Statement stmt) throws SQLException {
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + DATEREPEAT_EQUALS_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "@CODE boolean isEqualTo(String original, String compareTo) {\n"
				+ "		if (original==null||compareTo==null){return false;}\n"
				+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
				+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
				+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
				+ "			System.out.println(\"SPLITORIGINAL \"+i+\": \"+splitOriginal[i]);\n"
				+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
				+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
				+ "			if (intOriginal > intCompareTo) {\n"
				+ "				return false;\n"
				+ "			}\n"
				+ "			if (intOriginal < intCompareTo) {\n"
				+ "				return false;\n"
				+ "			}\n"
				+ "		}\n"
				+ "		return true;\n"
				+ "	} $$;");
	}

	protected static void addGreaterThanEquals(Statement stmt) throws SQLException {
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + DATEREPEAT_GREATERTHANEQUALS_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "@CODE boolean isEqualTo(String original, String compareTo) {\n"
				+ "		if (original==null||compareTo==null){return false;}\n"
				+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
				+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
				+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
				+ "			System.out.println(\"SPLITORIGINAL \"+i+\": \"+splitOriginal[i]);\n"
				+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
				+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
				+ "			if (intOriginal > intCompareTo) {\n"
				+ "				return true;\n"
				+ "			}\n"
				+ "			if (intOriginal < intCompareTo) {\n"
				+ "				return false;\n"
				+ "			}\n"
				+ "		}\n"
				+ "		return true;\n"
				+ "	} $$;");
	}

	protected static void addGreaterThan(Statement stmt) throws SQLException {
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + DATEREPEAT_GREATERTHAN_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "@CODE boolean isEqualTo(String original, String compareTo) {\n"
				+ "		if (original==null||compareTo==null){return false;}\n"
				+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
				+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
				+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
				+ "			System.out.println(\"SPLITORIGINAL \"+i+\": \"+splitOriginal[i]);\n"
				+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
				+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
				+ "			if (intOriginal > intCompareTo) {\n"
				+ "				return true;\n"
				+ "			}\n"
				+ "			if (intOriginal < intCompareTo) {\n"
				+ "				return false;\n"
				+ "			}\n"
				+ "		}\n"
				+ "		return false;\n"
				+ "	} $$;");
	}

	protected static void addLessThanEquals(Statement stmt) throws SQLException {
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + DATEREPEAT_LESSTHANEQUALS_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "@CODE boolean isEqualTo(String original, String compareTo) {\n"
				+ "		if (original==null||compareTo==null){return false;}\n"
				+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
				+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
				+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
				+ "			System.out.println(\"SPLITORIGINAL \"+i+\": \"+splitOriginal[i]);\n"
				+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
				+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
				+ "			if (intOriginal > intCompareTo) {\n"
				+ "				return false;\n"
				+ "			}\n"
				+ "			if (intOriginal < intCompareTo) {\n"
				+ "				return true;\n"
				+ "			}\n"
				+ "		}\n"
				+ "		return true;\n"
				+ "	} $$;");
	}

	protected static void addLessThan(Statement stmt) throws SQLException {
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + DATEREPEAT_LESSTHAN_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "@CODE boolean isEqualTo(String original, String compareTo) {\n"
				+ "		if (original==null||compareTo==null){return false;}\n"
				+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
				+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
				+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
				+ "			System.out.println(\"SPLITORIGINAL \"+i+\": \"+splitOriginal[i]);\n"
				+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
				+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
				+ "			if (intOriginal > intCompareTo) {\n"
				+ "				return false;\n"
				+ "			}\n"
				+ "			if (intOriginal < intCompareTo) {\n"
				+ "				return true;\n"
				+ "			}\n"
				+ "		}\n"
				+ "		return false;\n"
				+ "	} $$;");
	}

	protected static void addYearPart(Statement stmt) throws SQLException {
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + DATEREPEAT_YEAR_PART_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "Integer getYearPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*P([-0-9.]+)Y.*\", \"$1\"));\n"
				+ "	} $$;");
	}

	protected static void addMonthPart(Statement stmt) throws SQLException {
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + DATEREPEAT_MONTH_PART_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "Integer getMonthPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*Y([-0-9.]+)M.*\", \"$1\"));\n"
				+ "	} $$;");
	}

	protected static void addDayPart(Statement stmt) throws SQLException {
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + DATEREPEAT_DAY_PART_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "Integer getDayPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*M([-0-9.]+)D.*\", \"$1\"));\n"
				+ "	} $$;");
	}

	protected static void addHourPart(Statement stmt) throws SQLException {
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + DATEREPEAT_HOUR_PART_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "Integer getHourPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*D([-0-9.]+)h.*\", \"$1\"));\n"
				+ "	} $$;");
	}

	protected static void addMinutePart(Statement stmt) throws SQLException {
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + DATEREPEAT_MINUTE_PART_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "Integer getMinutePart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*h([-0-9.]+)n.*\", \"$1\"));\n"
				+ "	} $$;");
	}

	protected static void addSecondPart(Statement stmt) throws SQLException {
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + DATEREPEAT_SECOND_PART_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "Integer getSecondPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
				+ "		return Double.valueOf(dateRepeatStr.replaceAll(\".*n([-0-9.]+)s.*\", \"$1\")).intValue();\n"
				+ "	} $$;");
	}
}
