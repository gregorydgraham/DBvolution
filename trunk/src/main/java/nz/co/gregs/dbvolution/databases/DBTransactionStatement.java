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
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBScript;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Extends DBStatement to add support for database transactions.
 *
 * <p>
 * Use {@link DBScript} to easily create a transaction.
 *
 * <p>
 * You should not need to create on of these as statements and transactions are
 * managed by DBDatabase automatically.
 *
 * <p>
 * Transactions are a collection of database actions that have a coherent
 * collective nature. This implies that even though they are separate java
 * statements that they should be handled collectively by the database.
 *
 * @author Gregory Graham
 */
public class DBTransactionStatement extends DBStatement {

	private static final Log log = LogFactory.getLog(DBTransactionStatement.class);

	/**
	 * Creates a DBTransactionStatement for the given DBDatabase and
	 * DBStatement.
	 *
	 * <p>
	 * Used within {@link DBDatabase#doTransaction(nz.co.gregs.dbvolution.transactions.DBTransaction)
	 * } to create a transaction.
	 *
	 
	 
	  1 Database exceptions may be thrown
	 * @param database database
	 * @param statement statement
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public DBTransactionStatement(DBDatabase database, DBStatement statement) throws SQLException {
		super(database, statement.getConnection());
	}

	/**
	 * Closes the internal statement and creates a new statement for the next
	 * operation.
	 *
	 * <p>
	 * To close a transaction call the {@link #transactionFinished() } method.
	 * @throws java.sql.SQLException	 SQLException
	 */
	@Override
	public void close() throws SQLException {
		try {
			getInternalStatement().close();
		} catch (SQLException ex) {
			try {
				getInternalStatement().close();
			} catch (SQLException ex1) {
				log.info("Exception while closing transaction, continuing regardless.", ex);
			}
		}
		try {
			setInternalStatement(getConnection().createStatement());
		} catch (SQLException ex) {
			try {
				setInternalStatement(getConnection().createStatement());
			} catch (SQLException ex1) {
				throw ex;
			}
		}
	}

	/**
	 * Performs actions required following the completion of a transaction.
	 *
	 * <p>
	 * Transactions last longer than the standard DBStatement so a new method is
	 * required to close their resources.
	 *
	  1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public void transactionFinished() throws SQLException {
//		getInternalStatement().close();
		super.close();
	}
}
