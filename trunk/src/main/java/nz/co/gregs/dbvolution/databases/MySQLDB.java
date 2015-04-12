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
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.MySQLDBDefinition;

/**
 * A DBDatabase tweaked for MySQL databases
 *
 * @author Gregory Graham
 */
public class MySQLDB extends DBDatabase implements SupportsPolygonDatatype{

	private final static String MYSQLDRIVERNAME = "com.mysql.jdbc.Driver";

	/**
	 * Creates a {@link DBDatabase } instance for the data source.
	 *
	 * @param ds	 ds	
	 * @throws java.sql.SQLException	
	 */
	public MySQLDB(DataSource ds) throws SQLException {
		super(new MySQLDBDefinition(), ds);
	}

	/**
	 * Creates DBDatabase suitable for use with MySQL attached to the supplied
	 * JDBC URL, username, and password.
	 *
	 * @param jdbcURL jdbcURL
	 * @param password password
	 * @param username username
	 * @throws java.sql.SQLException
	 */
	public MySQLDB(String jdbcURL, String username, String password) throws SQLException {
		super(new MySQLDBDefinition(), MYSQLDRIVERNAME, jdbcURL, username, password);
	}

	/**
	 * Creates DBDatabase suitable for use with MySQL attached to the supplied
	 * JDBC URL, username, and password.
	 *
	 * @param server the server to connect to.
	 * @param port the port to connect on.
	 * @param databaseName the database that is required on the server.
	 * @param username the user to login as.
	 * @param password the password required to login successfully.
	 * @throws java.sql.SQLException
	 */
	public MySQLDB(String server, long port, String databaseName, String username, String password) throws SQLException {
		super(new MySQLDBDefinition(),
				MYSQLDRIVERNAME,
				"jdbc:mysql://" + server + ":" + port + "/" + databaseName,
				username,
				password);
		this.setDatabaseName(databaseName);
	}

	@Override
	public boolean supportsFullOuterJoinNatively() {
		return false;
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean supportsRecursiveQueriesNatively() {
		return false;
	}

}
