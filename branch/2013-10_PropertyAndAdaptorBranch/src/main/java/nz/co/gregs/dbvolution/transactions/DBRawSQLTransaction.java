/*
 * Copyright 2013 gregory.graham.
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
package nz.co.gregs.dbvolution.transactions;

import java.sql.Statement;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBStatement;

public class DBRawSQLTransaction implements DBTransaction<Boolean> {

    private final String sql;

    public DBRawSQLTransaction(String rawSQL) {
        this.sql = rawSQL;
    }

    @Override
    public Boolean doTransaction(DBDatabase dbDatabase) throws Exception {
        DBStatement dbStatement = dbDatabase.getDBStatement();
        try {
            dbStatement.addBatch(sql);
            int[] executeBatchResults = dbStatement.executeBatch();
            for (int result : executeBatchResults) {
                if (result == Statement.EXECUTE_FAILED) {
                    return Boolean.FALSE;
                }
            }
        } finally {
            dbStatement.close();
        }
        return Boolean.TRUE;
    }

}
