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
package nz.co.gregs.dbvolution.databases;

import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.definitions.Oracle11XEDBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.Oracle12DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.OracleDBDefinition;
import nz.co.gregs.dbvolution.databases.settingsbuilders.Oracle11XESettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.internal.oracle.xe.*;

/**
 * Implements support for version 11 and prior of the Oracle database.
 *
 * @author Gregory Graham
 * @see OracleDB
 * @see Oracle12DB
 * @see OracleDBDefinition
 * @see Oracle11XEDBDefinition
 * @see Oracle12DBDefinition
 */
public class Oracle11XEDB extends OracleDB {

	public static final long serialVersionUID = 1l;

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11 and above.
	 *
	 * @param dataSource a datasource to an Oracle database
	 * @throws java.sql.SQLException database errors
	 */
	public Oracle11XEDB(DataSource dataSource) throws SQLException {
		super(
				new Oracle11XESettingsBuilder().setDataSource(dataSource)
		);
	}

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @throws java.sql.SQLException database errors
	 */
	public Oracle11XEDB(DatabaseConnectionSettings dcs) throws SQLException {
		this(new Oracle11XESettingsBuilder().fromSettings(dcs));
	}

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @throws java.sql.SQLException database errors
	 */
	public Oracle11XEDB(Oracle11XESettingsBuilder dcs) throws SQLException {
		super(dcs);
	}

	/**
	 * Creates an Oracle connection for the DatabaseConnectionSettings.
	 *
	 * @param dcs	dcs
	 * @param defn the oracle database definition
	 * @throws java.sql.SQLException database errors
	 */
	public Oracle11XEDB(Oracle11XEDBDefinition defn, DatabaseConnectionSettings dcs) throws SQLException {
		this(new Oracle11XESettingsBuilder().fromSettings(dcs).setDefinition(defn));
	}

	/**
	 * Creates a DBDatabase instance tweaked for Oracle 11.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException database errors
	 */
	public Oracle11XEDB(String jdbcURL, String username, String password) throws SQLException {
		this(new Oracle11XESettingsBuilder().fromJDBCURL(jdbcURL, username, password));
	}

	@Override
	public void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
		super.addDatabaseSpecificFeatures(statement);
		for (GeometryFunctions fn : GeometryFunctions.values()) {
			fn.add(statement);
		}
		for (Point2DFunctions fn : Point2DFunctions.values()) {
			fn.add(statement);
		}
		for (MultiPoint2DFunctions fn : MultiPoint2DFunctions.values()) {
			fn.add(statement);
		}
		for (Line2DFunctions fn : Line2DFunctions.values()) {
			fn.add(statement);
		}
	}

	@Override
	public Oracle11XESettingsBuilder getURLInterpreter() {
		return new Oracle11XESettingsBuilder();
	}

}
