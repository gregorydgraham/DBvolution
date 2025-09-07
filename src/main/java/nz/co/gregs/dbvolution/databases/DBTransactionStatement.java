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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBScript;
import nz.co.gregs.dbvolution.databases.connections.DBConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Extends DBStatement to add support for database transactions.
 *
 * <p>
 * Use {@link DBScript} to easily create a transaction.
 *
 * <p>
 * You should not need to create one of these as statements and transactions are
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

	private static final Log LOG = LogFactory.getLog(DBTransactionStatement.class);

	/**
	 * Creates a DBTransactionStatement for the given DBDatabase and DBStatement.
	 *
	 * <p>
	 * Used within {@link DBDatabase#doTransaction(nz.co.gregs.dbvolution.transactions.DBTransaction)
   * } to create a transaction.
   *
   * <P>
   * Transaction always require explicit commit or rollback commands, though generally they encapsulated in the {@link DBScript#implement(nz.co.gregs.dbvolution.databases.DBDatabase)
   * } and {@link DBScript#test(nz.co.gregs.dbvolution.databases.DBDatabase) } methods</P>
   *
   * <ul><li> Database exceptions may be thrown</li></ul>
   *
   * @param database database
   * @param statement
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public DBTransactionStatement(DBDatabase database, DBStatement statement) throws SQLException {
		super(database, statement.getConnection());
    setAutoCommit(false);
	}

  @Override
  public synchronized DBConnection getConnection() throws SQLException {
    DBConnection conn = super.getConnection();
    return conn;
  }

	/**
	 * Closes the internal statement and creates a new statement for the next
	 * operation.
	 *
	 * <p>
	 * To close a transaction call the {@link #transactionFinished() } method.
	 *
	 * @throws java.sql.SQLException	SQLException
	 */
	@SuppressFBWarnings(
			value = "OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE", 
			justification = "We try twice, is there a better way to do this?")
	@Override
	public void close() throws SQLException {
		try {
			getInternalStatement().close();
		} catch (SQLException ex) {
			try {
				getInternalStatement().close();
			} catch (SQLException ex1) {
				LOG.info("Exception while closing transaction, continuing regardless.");
			}
		}
		if (database.getDefinition().willCloseConnectionOnStatementCancel()) {
			this.replaceBrokenConnection();
		} else {
			try {
				setInternalStatement(getConnection().getInternalStatement());
			} catch (Exception ex) {
				try {
					setInternalStatement(getConnection().getInternalStatement());
				} catch (Exception ex1) {
					throw new SQLException(ex);
				}
			}
		}
	}

  /**
   * Cancels this Statement object if both the DBMS and driver support aborting an SQL statement.This method can be used by one thread to cancel a statement
   * that is being executed by another thread.
   *
   * PLEASE NOTE THIS IS AN ABORT MECHANISM.
   *
   * Database exceptions may be thrown
   *
   * @throws SQLException
   */
  @SuppressFBWarnings(
          value = "OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE",
          justification = "We try twice, is there a better way to do this?")
  @Override
  public synchronized void cancel() throws SQLException {
    try {
      rollback();
      getInternalStatement().cancel();
    } catch (Exception ex) {
      try {
        getInternalStatement().cancel();
      } catch (SQLException ex1) {
        LOG.info("Exception while closing transaction, continuing regardless.");
      }
    }
    if (database.getDefinition().willCloseConnectionOnStatementCancel()) {
      this.replaceBrokenConnection();
    } else {
      try {
        setInternalStatement(getConnection().getInternalStatement());
      } catch (Exception ex) {
        try {
          setInternalStatement(getConnection().getInternalStatement());
        } catch (SQLException ex1) {
          throw new SQLException(ex);
        }
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
	 * 1 Database exceptions may be thrown
	 *
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
  public void transactionFinished() throws SQLException {
    // Discard anything that hasn't been committed.
    try{
      rollback();
    } catch (SQLException ex) {
      // I'm not worried about anything going wrong here
    }
    close();
  }


  public void commit() throws SQLException {
    getConnection().commit();
  }

  public void rollback() throws SQLException {
    getConnection().rollback();
  }
}
