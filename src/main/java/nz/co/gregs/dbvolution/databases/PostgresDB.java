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

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import nz.co.gregs.dbvolution.DBDatabase;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.PostgresDBDefinition;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.internal.postgres.Line2DFunctions;
import nz.co.gregs.dbvolution.internal.postgres.MultiPoint2DFunctions;
import nz.co.gregs.dbvolution.internal.postgres.StringFunctions;

/**
 * A DBDatabase tweaked for PostgreSQL.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class PostgresDB extends DBDatabase implements SupportsPolygonDatatype{

	private static final String POSTGRES_DRIVER_NAME = "org.postgresql.Driver";

	/**
	 * The default port number used by PostgreSQL.
	 */
	public static final int POSTGRES_DEFAULT_PORT = 5432;

	/**
	 * The default username used by PostgreSQL.
	 */
	public static final String POSTGRES_DEFAULT_USERNAME = "postgres";

/**
	 *
	 * Provides a convenient constructor for DBDatabases that have configuration
	 * details hardwired or are able to automatically retrieve the details.
	 *
	 * <p>
	 * This constructor creates an empty DBDatabase with only the default
	 * settings, in particular with no driver, URL, username, password, or
	 * {@link DBDefinition}
	 *
	 * <p>
	 * Most programmers should not call this constructor directly. Instead you
	 * should define a no-parameter constructor that supplies the details for
	 * creating an instance using a more complete constructor.
	 *
	 * <p>
	 * DBDatabase encapsulates the knowledge of the database, in particular the
	 * syntax of the database in the DBDefinition and the connection details from
	 * a DataSource.
	 *
	 * @see DBDefinition
	 */
	protected PostgresDB() {
		super();
	}
	/**
	 * Creates a PostgreSQL connection for the DataSource.
	 *
	 * @param ds	ds
	 */
	public PostgresDB(DataSource ds) {
		super(new PostgresDBDefinition(), ds);
	}

	/**
	 * Creates a PostgreSQL connection for the JDBC URL, username, and password.
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 */
	public PostgresDB(String jdbcURL, String username, String password) {
		super(new PostgresDBDefinition(), POSTGRES_DRIVER_NAME, jdbcURL, username, password);
	}

	/**
	 * Creates a PostgreSQL connection to the server on the port supplied, using
	 * the username and password supplied.
	 *
	 * @param hostname hostname
	 * @param port port
	 * @param databaseName databaseName
	 * @param username username
	 * @param password password
	 */
	public PostgresDB(String hostname, int port, String databaseName, String username, String password) {
		this(hostname, port, databaseName, username, password, null);
	}

	/**
	 * Creates a PostgreSQL connection to the server on the port supplied, using
	 * the username and password supplied.
	 *
	 * <p>
	 * Extra parameters to be added to the JDBC URL can be included in the
	 * urlExtras parameter.
	 *
	 * @param hostname hostname
	 * @param password password
	 * @param databaseName databaseName
	 * @param port port
	 * @param username username
	 * @param urlExtras urlExtras
	 */
	public PostgresDB(String hostname, int port, String databaseName, String username, String password, String urlExtras) {
		super(new PostgresDBDefinition(),
				POSTGRES_DRIVER_NAME,
				"jdbc:postgresql://" + hostname + ":" + port + "/" + databaseName + (urlExtras == null || urlExtras.isEmpty() ? "" : "?" + urlExtras),
				username, password);
		this.setDatabaseName(databaseName);
	}

	/**
	 * Creates a PostgreSQL connection to local computer("localhost") on the
	 * default port(5432) using the username and password supplied.
	 *
	 * <p>
	 * Extra parameters to be added to the JDBC URL can be included in the
	 * urlExtras parameter.
	 *
	 * @param databaseName databaseName
	 * @param username username
	 * @param password password
	 * @param urlExtras urlExtras
	 */
	public PostgresDB(String databaseName, String username, String password, String urlExtras) {
		this("localhost", POSTGRES_DEFAULT_PORT, databaseName, username, password, urlExtras);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Assumes that the database and application are on the the same machine.
	 *
	 * @param table the table to be loaded
	 * @param file the file to load data from
	 * @param delimiter the separator between the values of each row
	 * @param nullValue the string that represents NULL in this file.
	 * @param escapeCharacter the character that escapes special values
	 * @param quoteCharacter the character the surrounds strings.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return either (1) the row count for SQL Data Manipulation Language (DML)
	 * statements or (2) 0 for SQL statements that return nothing 1 Database
	 * exceptions may be thrown
	 * @throws SQLException database exceptions may be thrown.
	 */
	public int loadFromCSVFile(DBRow table, File file, String delimiter, String nullValue, String escapeCharacter, String quoteCharacter) throws SQLException {
		int returnValue = 0;
		final DBStatement dbStatement = this.getDBStatement();
		try {
			returnValue = dbStatement.executeUpdate("COPY " + table.getTableName() + " FROM '" + file.getAbsolutePath() + "' WITH (DELIMITER '" + delimiter + "', NULL '" + nullValue + "', ESCAPE '" + escapeCharacter + "', FORMAT csv, QUOTE '" + quoteCharacter + "');");
		} finally {
			dbStatement.close();
		}
		return returnValue;
	}

	/**
	 * Create a new database/schema on this database server.
	 *
	 * <p>
	 * Generally requires all sorts of privileges and is best performed by
	 * database administrator (DBA).
	 *
	 * @param databaseName the name of the new database
	 * @throws SQLException database exceptions may be thrown
	 */
	public void createDatabase(String databaseName) throws SQLException {
		String sqlString = "CREATE DATABASE " + databaseName + ";";
		final DBStatement dbStatement = getDBStatement();
		try {
			dbStatement.execute(sqlString);
		} finally {
			dbStatement.close();
		}
	}

	/**
	 * Create a new database/schema on this database server.
	 *
	 * <p>
	 * Generally requires all sorts of privileges and is best performed by
	 * database administrator (DBA).
	 *
	 * @param username The user to be created
	 * @param password the password the user will use.
	 * @throws SQLException database exceptions may be throwns.
	 */
	public void createUser(String username, String password) throws SQLException {
		String sqlString = "CREATE USER \"" + username.replaceAll("\\\"", "") + "\" WITH PASSWORD '" + password.replaceAll("'", "") + "';";
		final DBStatement dbStatement = getDBStatement();
		try {
			dbStatement.execute(sqlString);
		} finally {
			dbStatement.close();
		}
	}

	@Override
	public <TR extends DBRow> void dropTableNoExceptions(TR tableRow) throws AccidentalDroppingOfTableException, AutoCommitActionDuringTransactionException {
		try {
			this.dropTable(tableRow);
		} catch (org.postgresql.util.PSQLException exp) {
//			exp.printStackTrace();
		} catch (SQLException exp) {
//			exp.printStackTrace();
		}
	}

	@Override
	protected void addDatabaseSpecificFeatures(Statement stmnt) throws SQLException {
		createPostGISExtension(stmnt);
		createPostGISTopologyExtension(stmnt);
		for (StringFunctions fn : StringFunctions.values()) {
			fn.add(stmnt);
		}
		for (Line2DFunctions fn : Line2DFunctions.values()) {
			fn.add(stmnt);
		}
		for (MultiPoint2DFunctions fn : MultiPoint2DFunctions.values()) {
			fn.add(stmnt);
		}
	}

	private void createPostGISTopologyExtension(Statement stmnt) {
		try {
			boolean execute = stmnt.execute("select * from pg_extension where extname = 'postgis_topology';");
			final ResultSet resultSet = stmnt.getResultSet();
			boolean postGISAlreadyCreated = resultSet.next();
			if (!postGISAlreadyCreated) {
				stmnt.execute("CREATE EXTENSION IF NOT EXISTS postgis_topology;");
			}
		} catch (SQLException sqlex) {
			System.out.println("" + sqlex.getMessage());;
		}
	}

	private void createPostGISExtension(Statement stmnt) {
		try {
			boolean execute = stmnt.execute("select * from pg_extension where extname = 'postgis';");
			final ResultSet resultSet = stmnt.getResultSet();
			boolean postGISAlreadyCreated = resultSet.next();
			if (!postGISAlreadyCreated) {
				stmnt.execute("CREATE EXTENSION IF NOT EXISTS postgis;");
			}
		} catch (SQLException sqlex) {
			System.out.println("" + sqlex.getMessage());;
		}
	}
}
