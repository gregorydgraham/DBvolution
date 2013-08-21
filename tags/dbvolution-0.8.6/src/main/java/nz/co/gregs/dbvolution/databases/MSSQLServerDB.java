/*
 * Copyright 2013 gregorygraham.
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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.sql.DataSource;

/**
 * Add this to the Maven pom to use:
 * <pre>
 * <dependency>
 * <groupId>com.microsoft</groupId>
 * <artifactId>sqljdbc</artifactId>
 * <version>1.0</version>
 * </dependency>
 * </pre>
 *
 *
 * @author Malcolm Lett
 * @author Gregory Graham
 */
public class MSSQLServerDB extends DBDatabase {
    private static final DateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    public final static String SQLSERVERDRIVERNAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";


    public MSSQLServerDB(DataSource ds) {
        super(ds);
    }

    public MSSQLServerDB(String driverName, String jdbcURL, String username, String password) {
        super(driverName, jdbcURL, username, password);
    }
    
    public MSSQLServerDB(String jdbcURL, String username, String password) {
        super(SQLSERVERDRIVERNAME, jdbcURL, username, password);
    }

    @Override
    public String getDateFormattedForQuery(Date date) {
        return "'" + DATETIME_FORMAT.format(date) + "'";
    }
}
