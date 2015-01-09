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
package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.transactions.DBTransaction;
import nz.co.gregs.dbvolution.actions.DBActionList;

/**
 * A convenient method of implement a database script in DBvolution.
 *
 * <p>
 * DBScript provides automatic transaction control for a collection of
 * DBvolution operations.
 *
 * <p>
 * Use {@link DBDatabase#test(nz.co.gregs.dbvolution.DBScript) } or
 * {@link DBScript#test(nz.co.gregs.dbvolution.DBDatabase)} to run the script
 * within a Read Only Transaction.
 *
 * <p>
 * Use {@link DBDatabase#implement(nz.co.gregs.dbvolution.DBScript)} or 
 * {@link DBScript#implement(nz.co.gregs.dbvolution.DBDatabase) } to run the script within a Committed Transaction.
 *
 * <p>
 * DBvolution is available on <a
 * href="https://sourceforge.net/projects/dbvolution/">SourceForge</a> complete
 * with <a href="https://sourceforge.net/p/dbvolution/blog/">BLOG</a>
 *
 * @author Gregory Graham
 */
public abstract class DBScript {

	/**
	 *
	 * Create all the database interaction is this method.
	 *
	 * Call test() or implement() to safely run the script within a transaction.
	 *
	 * Use the {@link DBActionList} to collect the script's actions for saving
	 * or later use.
	 *
	 * @param db	 db	
	 * @return DBActionList
	 * @throws java.lang.Exception java.lang.Exception
	 
	 */
	public abstract DBActionList script(DBDatabase db) throws Exception;

	/**
	 * Run the script in a committed transaction.
	 *
	 * <P>
	 * Implement() wraps the {@link #script(nz.co.gregs.dbvolution.DBDatabase) }
	 * method in a transaction and commits it.
	 *
	 * <P>
	 * Any exceptions will cause the script to abort and rollback safely.
	 *
	 * <P>
	 * When the script executes without exceptions the changes will be committed
	 * and made permanent.
	 *
	 * @param db	 db	
	 * @return a DBActionList of all the actions performed on the database
	 * @throws java.lang.Exception java.lang.Exception
	 
	 */
	public final DBActionList implement(DBDatabase db) throws Exception {
		DBTransaction<DBActionList> trans = getDBTransaction();
		DBActionList revertScript = db.doTransaction(trans);
		return revertScript;
	}

	/**
	 * Run the script in a read-only transaction.
	 *
	 * Test() wraps the {@link #script(nz.co.gregs.dbvolution.DBDatabase) }
	 * method in a transaction but rolls it back.
	 *
	 * <p>
	 * Any changes will be safely rolled back.
	 *
	 * <P>
	 * Any exceptions will cause the script to abort and rollback safely.
	 *
	 * @param db	 db	
	 * @return a DBActionList of all the actions performed on the database
	 * @throws java.lang.Exception java.lang.Exception
	 
	 */
	public final DBActionList test(DBDatabase db) throws Exception {
		DBTransaction<DBActionList> trans = getDBTransaction();
		DBActionList revertScript = db.doReadOnlyTransaction(trans);
		return revertScript;
	}

	/**
	 * Creates and returns a DBtransaction for this DBScript.
	 *
	 * @return the transaction required to run the script.
	 */
	public final DBTransaction<DBActionList> getDBTransaction() {
		return new DBTransaction<DBActionList>() {
			@Override
			public DBActionList doTransaction(DBDatabase dbd) throws Exception {
				DBActionList revertScript = script(dbd);
				return revertScript;
			}
		};
	}
}
