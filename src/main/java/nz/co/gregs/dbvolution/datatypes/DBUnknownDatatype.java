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
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;

/**
 *
 * Class to allow some functionality for data types that DBvolution does not yet
 * support
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBUnknownDatatype extends QueryableDatatype {

	private static final long serialVersionUID = 1L;

	/**
	 * The default constructor for a DBUnknownDatatype.
	 *
	 * <p>
	 * Literally does nothing.
	 */
	public DBUnknownDatatype() {
		super();
	}

	@Override
	public String getSQLDatatype() {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String formatValueForSQLStatement(DBDatabase db) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	void setValue(Object newLiteralValue) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public DBUnknownDatatype getQueryableDatatypeForExpressionValue() {
		return new DBUnknownDatatype();
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return new HashSet<DBRow>();
	}

//	@Override
//	public void setFromResultSet(DBDatabase database, ResultSet resultSet, String resultSetColumnName) throws SQLException {
//		removeConstraints();
//		if (resultSet == null || resultSetColumnName == null) {
//			this.setToNull();
//		} else {
//			String dbValue;
//			try {
//				dbValue = resultSet.getString(resultSetColumnName);
//				if (resultSet.wasNull()) {
//					dbValue = null;
//				}
//			} catch (SQLException ex) {
//				// Probably means the column wasn't selected.
//				dbValue = null;
//			}
//			if (dbValue == null) {
//				this.setToNull();
//			} else {
//				this.setLiteralValue(dbValue);
//			}
//		}
//		setUnchanged();
//		setDefined(true);
//		propertyWrapper = null;
//	}
	@Override
	protected Object getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException {
		return resultSet.getString(fullColumnName);
	}
}
