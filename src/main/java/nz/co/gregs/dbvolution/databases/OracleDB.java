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
package nz.co.gregs.dbvolution.databases;

import nz.co.gregs.dbvolution.internal.oracle.StringFunctions;
import java.sql.SQLException;
import java.sql.Statement;
import nz.co.gregs.dbvolution.databases.settingsbuilders.AbstractOracleSettingsBuilder;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.OracleDBDefinition;
import nz.co.gregs.dbvolution.databases.settingsbuilders.Oracle11XESettingsBuilder;
import nz.co.gregs.dbvolution.databases.supports.SupportsPolygonDatatype;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;
import nz.co.gregs.dbvolution.internal.query.StatementDetails;
import nz.co.gregs.regexi.Regex;
import static nz.co.gregs.dbvolution.databases.DBDatabaseImplementation.ResponseToException.*;
import static nz.co.gregs.dbvolution.databases.QueryIntention.*;

/**
 * Super class for connecting the different versions of the Oracle DB.
 *
 * <p>
 * You should probably use {@link Oracle11XEDB} or {@link Oracle12DB} instead.
 *
 * @author Gregory Graham
 * @see Oracle11XEDB
 * @see Oracle12DB
 */
public abstract class OracleDB extends DBDatabaseImplementation implements SupportsPolygonDatatype {

  public static final String ORACLE_JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
  public static final long serialVersionUID = 1l;
  public static final int DEFAULT_PORT = 1521;

  /**
   * Creates an Oracle connection for the DatabaseConnectionSettings.
   *
   * @param builder settings required to connect to the database server
   * @throws java.sql.SQLException database errors
   */
  public OracleDB(AbstractOracleSettingsBuilder<?, ?> builder) throws SQLException {
    super(builder);
  }

  /**
   * Creates an Oracle connection for the DatabaseConnectionSettings.
   *
   * @param dcs	dcs
   * @throws java.sql.SQLException database errors
   */
  @Deprecated
  public OracleDB(DatabaseConnectionSettings dcs) throws SQLException {
    this(new OracleDBDefinition(), dcs);
  }

  /**
   * Creates an Oracle connection for the DatabaseConnectionSettings.
   *
   * @param dcs	dcs
   * @param defn the oracle database definition
   * @throws java.sql.SQLException database errors
   */
  @Deprecated
  public OracleDB(OracleDBDefinition defn, DatabaseConnectionSettings dcs) throws SQLException {
    super(new Oracle11XESettingsBuilder().fromSettings(dcs).setDefinition(defn));
  }

  /**
   * Creates a DBDatabase instance for the definition and data source.
   *
   * <p>
   * You should probably be using {@link Oracle11XEDB#Oracle11XEDB(java.lang.String, int, java.lang.String, java.lang.String, java.lang.String)
   * } or {@link Oracle12DB#Oracle12DB(java.lang.String, int, java.lang.String, java.lang.String, java.lang.String)}
   *
   * @param definition definition
   * @param driverName the database driver class name
   * @param password password
   * @param jdbcURL jdbcURL
   * @param username username
   * @throws java.sql.SQLException database errors
   */
  @Deprecated
  public OracleDB(DBDefinition definition, String driverName, String jdbcURL, String username, String password) throws SQLException {
    this(new Oracle11XESettingsBuilder().fromJDBCURL(jdbcURL, username, password).setDefinition(definition).setDriverName(driverName));
  }

  /**
   * Creates a DBDatabase instance.
   *
   * @param dbDefinition an oracle database definition instance
   * @param dataSource a data source to an Oracle database
   * @throws java.sql.SQLException database errors
   */
  @Deprecated
  public OracleDB(DBDefinition dbDefinition, AbstractOracleSettingsBuilder<?, ?> dataSource) throws SQLException {
    this(dbDefinition, ORACLE_JDBC_DRIVER, dataSource);
  }

  /**
   * Creates a DBDatabase instance.
   *
   * @param dbDefinition an oracle database definition instance
   * @param driverName the database driver class name
   * @param dataSource a data source to an Oracle database
   * @throws java.sql.SQLException database errors
   */
  @Deprecated
  public OracleDB(DBDefinition dbDefinition, String driverName, AbstractOracleSettingsBuilder<?, ?> dataSource) throws SQLException {
    super(dataSource.setDefinition(dbDefinition).setDriverName(driverName));
  }

  @Override
  public void addDatabaseSpecificFeatures(Statement statement) throws ExceptionDuringDatabaseFeatureSetup {
    for (StringFunctions fn : StringFunctions.values()) {
      try {
        fn.add(statement);
      } catch (Exception ex) {
        throw new ExceptionDuringDatabaseFeatureSetup("FAILED TO ADD FEATURE: " + fn.name(), ex);
      }
    }
  }

  private final static Regex SEQUENCE_DOES_NOT_EXIST_REGEX = Regex.empty().literal("ORA-02289: sequence does not exist").toRegex();
  private final static Regex TRIGGER_DOES_NOT_EXIST_REGEX = Regex.empty().literal("ORA-04080: trigger ").anyCharacter().optionalManyGreedy().literal(" does not exist").toRegex();
  private final static Regex TABLE_ALREADY_EXISTS_REGEX = Regex.empty().literal("ORA-00955: name is already used by an existing object").toRegex();
  private final static Regex TABLE_DOES_NOT_EXIST_REGEX = Regex.empty().literal("ORA-00942: table or view does not exist").toRegex();
  private final static Regex LOOP_IN_RECURSIVE_QUERY_REGEX = Regex.empty().literal("ORA-32044: cycle detected while executing recursive WITH query").toRegex();
  private final static Regex COLUMN_ALREADY_EXISTS_REGEX = Regex.startingFromTheBeginning().literal("ORA-01430: column being added already exists in table").toRegex();
  private static QueryExceptionHandler[] exceptionHandlers = new QueryExceptionHandler[0];

  @Override
  public ResponseToException addFeatureToFixException(SQLException exp, QueryIntention intent, StatementDetails details) throws SQLException {

    if (exceptionHandlers.length == 0) {
      exceptionHandlers = new QueryExceptionHandler[]{
        new QueryExceptionHandler(SKIPQUERY, TABLE_DOES_NOT_EXIST_REGEX, CHECK_TABLE_EXISTS, DROP_TABLE),
        new QueryExceptionHandler(SKIPQUERY, SEQUENCE_DOES_NOT_EXIST_REGEX, DROP_TABLE, DROP_SEQUENCE, CREATE_TABLE, CREATE_TRIGGER_BASED_IDENTITY),
        new QueryExceptionHandler(SKIPQUERY, COLUMN_ALREADY_EXISTS_REGEX, ADD_COLUMN_TO_TABLE, ALTER_TABLE_ADD_COLUMN),
        new QueryExceptionHandler(SKIPQUERY, TABLE_ALREADY_EXISTS_REGEX),
        new QueryExceptionHandler(SKIPQUERY, TRIGGER_DOES_NOT_EXIST_REGEX),
        new QueryExceptionHandler(EMULATE_RECURSIVE_QUERY, LOOP_IN_RECURSIVE_QUERY_REGEX)
      };
    }
    ResponseToException response = QueryExceptionHandler.handle(exceptionHandlers, intent, exp);
    if (response == null || NOT_HANDLED.equals(response)) {
      return super.addFeatureToFixException(exp, intent, details);
    } else {
      return response;
    }
  }

  @Override
  public Integer getDefaultPort() {
    return 1521;
  }

}
