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
import nz.co.gregs.dbvolution.DBDatabase;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.definitions.MSSQLServerDBDefinition;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.internal.sqlserver.*;

/**
 * A DBDatabase object tweaked to work with Microsoft SQL Server.
 *
 * <p>
 * Remember to include the MS SQL Server JDBC driver in your classpath.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Malcolm Lett
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class MSSQLServerDB extends DBDatabase implements SupportsPolygonDatatype{

	/**
	 * The Microsoft Driver used to connect to MS SQLServer databases.
	 */
	public final static String SQLSERVERDRIVERNAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

	/**
	 * The JTDS Driver to use to connect to MS SQLServer databases.
	 */
	public final static String JTDSDRIVERNAME = "net.sourceforge.jtds.jdbc.Driver";

	/**
	 * The default port used by MS SQLServer databases.
	 */
	public final static int DEFAULT_PORT_NUMBER = 1433;

	/**
	 * Creates a {@link DBDatabase } instance for the MS SQL Server data source.
	 *
	 * @param ds	a DataSource to an MS SQLServer database
	 */
	public MSSQLServerDB(DataSource ds) {
		super(new MSSQLServerDBDefinition(), ds);
	}

	/**
	 * Creates a {@link DBDatabase } instance for MS SQL Server using the driver,
	 * JDBC URL, username, and password.
	 *
	 * @param driverName driverName
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 */
	public MSSQLServerDB(String driverName, String jdbcURL, String username, String password)  {
		super(new MSSQLServerDBDefinition(), driverName, jdbcURL, username, password);
	}

	/**
	 * Creates a {@link DBDatabase } instance for MS SQL Server using the JDBC
	 * URL, username, and password.
	 *
	 * <p>
	 * The default driver will be used for the connection.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 */
	public MSSQLServerDB(String jdbcURL, String username, String password)  {
		super(new MSSQLServerDBDefinition(), SQLSERVERDRIVERNAME, jdbcURL, username, password);
	}

	/**
	 * Connect to an MS SQLServer database using the connection details specified
	 * and Microsoft's driver.
	 *
	 * @param hostname the name of the server where the database resides
	 * @param instanceName the name of the particular database instance to connect to  (can be null)
	 * @param databaseName the name of the database within the instance (can be null)
	 * @param portNumber the port number that the database is available on 
	 * @param username the account to connect via
	 * @param password the password to identify username.
	 */
	public MSSQLServerDB(String hostname, String instanceName, String databaseName, int portNumber, String username, String password) {
		super(
				new MSSQLServerDBDefinition(),
				SQLSERVERDRIVERNAME,
				"jdbc:sqlserver://" + hostname + (instanceName != null ? "\\" + instanceName : "") + ":" + portNumber + ";" + (databaseName == null ? "" : "databaseName=" + databaseName + ";"),
				username,
				password
		);
	}

	/**
	 * Connect to an MS SQLServer database using the connection details specified
	 * and Microsoft's driver.
	 *
	 * @param driverName the JDBC driver class to use.
	 * @param hostname the name of the server where the database resides.
	 * @param instanceName the name of the particular database instance to connect to  (can be null).
	 * @param databaseName the name of the database within the instance (can be null).
	 * @param portNumber the port number that the database is available on .
	 * @param username the account to connect via.
	 * @param password the password to identify username.
	 */
	public MSSQLServerDB(String driverName, String hostname, String instanceName, String databaseName, int portNumber, String username, String password) {
		super(
				new MSSQLServerDBDefinition(),
				driverName,
				"jdbc:sqlserver://" + hostname + (instanceName != null ? "\\" + instanceName : "") + ":" + portNumber + ";" + (databaseName == null ? "" : "databaseName=" + databaseName + ";"),
				username,
				password
		);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	protected void addDatabaseSpecificFeatures(Statement statement) throws SQLException {
		for (Point2DFunctions fn : Point2DFunctions.values()) {
			fn.add(statement);
		}
		for (Line2DFunctions fn : Line2DFunctions.values()) {
			fn.add(statement);
		}
		for (MultiPoint2DFunctions fn : MultiPoint2DFunctions.values()) {
			fn.add(statement);
		}
	}
}
