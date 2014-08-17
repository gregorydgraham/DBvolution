/*
 * Copyright 2014 greg.
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
package nz.co.gregs.dbvolution.exceptions;

import java.sql.SQLException;
import javax.sql.DataSource;

/**
 *
 * @author greg
 */
public class UnableToCreateDatabaseConnectionException extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	public UnableToCreateDatabaseConnectionException(String jdbcURL, String username, SQLException noConnection) {
		super("Unable to create a Database Connection: please check the database URL, username, and password, and that the appropriate libaries have been supplied: URL=" + jdbcURL + " USERNAME=" + username, noConnection);
	}

	public UnableToCreateDatabaseConnectionException(DataSource dataSource, SQLException noConnection) {
		super("Unable to create a Database Connection: please check the database URL, username, and password, and that the appropriate libaries have been supplied: DATASOURCE=" + dataSource.toString(), noConnection);
	}

}
