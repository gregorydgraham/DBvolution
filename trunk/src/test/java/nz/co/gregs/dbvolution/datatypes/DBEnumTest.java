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
package nz.co.gregs.dbvolution.datatypes;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.List;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.dbvolution.datatypes.DBEnumTest.IntegerEnumTable.RecordType;
import nz.co.gregs.dbvolution.datatypes.DBEnumTest.StringEnumTable.StringEnumType;

import org.junit.Ignore;
import org.junit.Test;

public class DBEnumTest extends AbstractTest {

	public DBEnumTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void createRecordUsingEnum() {
		IntegerEnumTable row = new IntegerEnumTable();
		row.recordType.setValue(RecordType.MOVEMENT_CANCELLATION_REQUEST); // nested class imported
		row.recordType.setValue(IntegerEnumTable.RecordType.MOVEMENT_CANCELLATION_REQUEST); // explicit reference to nested class

		String sqlFragment = row.recordType.toSQLString(database);
		assertThat(sqlFragment, is("3"));
		System.out.println(sqlFragment);
	}

	@Test
	public void createRecordUsingLiteral() {
		IntegerEnumTable row = new IntegerEnumTable();
		row.recordType.setLiteralValue(
				IntegerEnumTable.RecordType.MOVEMENT_CANCELLATION_REQUEST.getCode());

		String sqlFragment = row.recordType.toSQLString(database);
		assertThat(sqlFragment, is("3"));
		System.out.println(sqlFragment);
	}

	@Test
	public void filterRecordUsingEnum() {
		IntegerEnumTable rowExemplar = new IntegerEnumTable();
		rowExemplar.recordType.permittedValues(
				RecordType.MOVEMENT_REQUEST_RECORD,
				RecordType.SHIPPING_MANIFEST_RECORD);

		String sqlFragment = rowExemplar.recordType.getWhereClause(database, "column");
		System.out.println(sqlFragment);
		assertThat(sqlFragment, containsString("column in ( 2 , 1 )"));
	}

	@Test
	public void filterRecordUsingLiteral() {
		IntegerEnumTable rowExemplar = new IntegerEnumTable();
		rowExemplar.recordType.permittedValues(
				RecordType.MOVEMENT_REQUEST_RECORD.getCode(),
				RecordType.SHIPPING_MANIFEST_RECORD.getCode());

		String sqlFragment = rowExemplar.recordType.getWhereClause(database, "column");
		System.out.println(sqlFragment);
		assertThat(sqlFragment, containsString("column in ( 2 , 1 )"));
	}

	@Test
	public void processIntegerRecord() throws SQLException {
		final IntegerEnumTable integerTableExemplar = new IntegerEnumTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(integerTableExemplar);
		database.preventDroppingOfTables(true);
		database.createTable(integerTableExemplar);
		try {
			database.insert(
					new IntegerEnumTable(1, RecordType.MOVEMENT_REQUEST_RECORD),
					new IntegerEnumTable(2, RecordType.SHIPPING_MANIFEST_RECORD),
					new IntegerEnumTable(4, RecordType.MOVEMENT_REQUEST_RECORD));

			integerTableExemplar.recordType.permittedValues(
					RecordType.MOVEMENT_CANCELLATION_REQUEST,
					RecordType.MOVEMENT_REQUEST_RECORD,
					RecordType.SHIPPING_MANIFEST_RECORD);
			List<IntegerEnumTable> rows = database.get(integerTableExemplar);
			database.print(rows);

			for (IntegerEnumTable row : rows) {
				if (row.uid_202.getValue().intValue() == 1) {
					assertThat(row.recordType.enumValue(), is(RecordType.MOVEMENT_REQUEST_RECORD));
				}
				if (row.uid_202.getValue().intValue() == 2) {
					assertThat(row.recordType.enumValue(), is(RecordType.SHIPPING_MANIFEST_RECORD));
				}
			}
		} finally {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(integerTableExemplar);
			database.preventDroppingOfTables(true);
		}
	}

	@Test
	public void processStringRecord() throws SQLException {
		final StringEnumTable stringTableExemplar = new StringEnumTable();
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(stringTableExemplar);
		database.preventDroppingOfTables(true);
		database.createTable(stringTableExemplar);
		try {
			database.insert(
					new StringEnumTable(1, StringEnumType.MOVEMENT_REQUEST_RECORD),
					new StringEnumTable(2, StringEnumType.SHIPPING_MANIFEST_RECORD),
					new StringEnumTable(4, StringEnumType.MOVEMENT_REQUEST_RECORD));

			stringTableExemplar.recordType.permittedValues(
					StringEnumType.MOVEMENT_CANCELLATION_REQUEST,
					StringEnumType.MOVEMENT_REQUEST_RECORD,
					StringEnumType.SHIPPING_MANIFEST_RECORD);
			List<StringEnumTable> rows = database.get(stringTableExemplar);
			database.print(rows);

			for (StringEnumTable row : rows) {
				if (row.uid_202.getValue().intValue() == 1) {
					assertThat(row.recordType.enumValue(), is(StringEnumType.MOVEMENT_REQUEST_RECORD));
				}
				if (row.uid_202.getValue().intValue() == 2) {
					assertThat(row.recordType.enumValue(), is(StringEnumType.SHIPPING_MANIFEST_RECORD));
				}
			}
		} finally {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(stringTableExemplar);
			database.preventDroppingOfTables(true);

		}
	}

	@Test
	public void correctlyConvertsLongToIntegerEnum() {
		// warm up enum type
		IntegerEnumTable row = new IntegerEnumTable();
		row.recordType.setValue(IntegerEnumTable.RecordType.SHIPPING_MANIFEST_RECORD);

		// do test
		long code = IntegerEnumTable.RecordType.MOVEMENT_CANCELLATION_REQUEST.code;
		row.recordType.setLiteralValue(code);
		assertThat(row.recordType.enumValue(), is(IntegerEnumTable.RecordType.MOVEMENT_CANCELLATION_REQUEST));
	}

	@Test
	public void displayInJsp() {
		/*
		 jsp: <%-- in a table --%>
		 <table>		
		 <c:forEach var="records" item="record">
		 <tr>
		 <td>${record.recordType.displayName}</td>
		 </tr>
		 </c:forEach>
		 </table>
		   
		 <%-- in a selection dropdown --%>
		 <form:select path="recordType">
		 <form:options items="${allRecordTypes}" itemLabel="displayName"/>
		 </form:select>
		 */
	}

	public static class IntegerEnumTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn("uid_202")
		@DBPrimaryKey
		public DBInteger uid_202 = new DBInteger();

		@DBColumn("c_5")
		public DBIntegerEnum<RecordType> recordType = new DBIntegerEnum<RecordType>();

		public IntegerEnumTable() {
		}

		public IntegerEnumTable(Integer uid, RecordType recType) {
			this.uid_202.setValue(uid);
			this.recordType.setValue(recType);
		}

		/**
		 * Valid values for {@link #recordType}.
		 *
		 * <p>
		 * Nested class to make it obvious which table the enum is for
		 */
		public static enum RecordType implements DBEnumValue<Integer> {

			SHIPPING_MANIFEST_RECORD(1, "Shipping Manifest Record"),
			MOVEMENT_REQUEST_RECORD(2, "Movement Request Record"),
			MOVEMENT_CANCELLATION_REQUEST(3, "Movement Cancellation Request");

			private int code;
			private String displayName;

			private RecordType(int code, String displayName) {
				this.code = code;
				this.displayName = displayName;
			}

			@Override
			public Integer getCode() {
				return code;
			}

			public String getDisplayName() {
				return displayName;
			}

			public static RecordType valueOfCode(DBInteger code) {
				return valueOfCode(code == null ? null : code.getValue().intValue());
			}

			public static RecordType valueOfCode(Integer code) {
				if (code == null) {
					return null;
				}
				for (RecordType recordType : values()) {
					if (recordType.getCode().equals(code)) {
						return recordType;
					}
				}
				throw new IllegalArgumentException("Invalid " + RecordType.class.getSimpleName() + " code: " + code);
			}
		}
	}

//    public static class LongTable extends DBRow {
//        private static final long serialVersionUID = 1L;
//
//        @DBColumn("uid_202")
//        @DBPrimaryKey
//        public DBInteger uid_202 = new DBInteger();
//        
//        @DBColumn("c_5")
//        public DBIntegerEnum<RecordType> recordType = new DBIntegerEnum<RecordType>();
//        
//        public LongTable() {
//        }
//
//        public LongTable(Integer uid, RecordType recType) {
//            this.uid_202.setLiteralValue(uid);
//            this.recordType.setLiteralValue(recType);
//        }
//
//        /**
//         * Valid values for {@link #recordType}
//         */
//        // Nested class to make it obvious which table the enum is for
//        public static enum RecordType implements DBEnumValue<Long> {
//            SHIPPING_MANIFEST_RECORD(1, "Shipping Manifest Record"),
//            MOVEMENT_REQUEST_RECORD(2, "Movement Request Record"),
//            MOVEMENT_CANCELLATION_REQUEST(3, "Movement Cancellation Request");
//            
//            private long literalValue;
//            private String displayName;
//
//            private RecordType(int code, String displayName) {
//                this.literalValue = code;
//                this.displayName = displayName;
//            }
//
//            @Override
//            public Long getLiteralValue() {
//                return literalValue;
//            }
//
//            public String getDisplayName() {
//                return displayName;
//            }
//
//            public static RecordType valueOfCode(DBInteger code) {
//                return valueOfCode(code == null ? null : code.longValue());
//            }
//
//            public static RecordType valueOfCode(Long code) {
//                if (code == null) {
//                    return null;
//                }
//                for (RecordType recordType : values()) {
//                    if (recordType.getLiteralValue() == code) {
//                        return recordType;
//                    }
//                }
//                throw new IllegalArgumentException("Invalid " + RecordType.class.getSimpleName() + " code: " + code);
//            }
//        }
//    }
	public static class StringEnumTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn("uid_203")
		@DBPrimaryKey
		public DBInteger uid_202 = new DBInteger();

		@DBColumn("c_5")
		public DBStringEnum<StringEnumType> recordType = new DBStringEnum<StringEnumType>();

		public StringEnumTable() {
		}

		public StringEnumTable(Integer uid, StringEnumType recType) {
			this.uid_202.setValue(uid);
			this.recordType.setValue(recType);
		}

		/**
		 * Valid values for {@link #recordType}
		 */
		// Nested class to make it obvious which table the enum is for
		public static enum StringEnumType implements DBEnumValue<String> {

			SHIPPING_MANIFEST_RECORD("MANRECORD", "Shipping Manifest Record"),
			MOVEMENT_REQUEST_RECORD("MOVEREQ", "Movement Request Record"),
			MOVEMENT_CANCELLATION_REQUEST("CANCREQ", "Movement Cancellation Request");
			private final String literalValue;
			private final String displayName;

			private StringEnumType(String code, String displayName) {
				this.literalValue = code;
				this.displayName = displayName;
			}

			@Override
			public String getCode() {
				return literalValue;
			}

			public String getDisplayName() {
				return displayName;
			}

			public static StringEnumType valueOfCode(DBString code) {
				return valueOfCode(code == null ? null : code.stringValue());
			}

			public static StringEnumType valueOfCode(String code) {
				if (code == null) {
					return null;
				}
				for (StringEnumType recordType : values()) {
					if (recordType.getCode().equals(code)) {
						return recordType;
					}
				}
				throw new IllegalArgumentException("Invalid " + StringEnumType.class.getSimpleName() + " code: " + code);
			}
		}
	}
}
