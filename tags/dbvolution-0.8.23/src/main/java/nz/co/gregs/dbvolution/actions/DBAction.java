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
package nz.co.gregs.dbvolution.actions;

import java.sql.SQLException;
import java.sql.Statement;
import nz.co.gregs.dbvolution.DBDatabase;

/**
 *
 * @author gregorygraham
 */
public abstract class DBAction {
    protected String sql = "";
//    protected final DBDatabase database;
    
    public DBAction() {
        super();
//        this.database = database;
    }
    
    public DBAction(String sql) {
        super();
        this.sql = sql;
//        this.database = database;
    }
    
    public String getSQLRepresentation(){
        return sql;
    }

    public abstract boolean canBeBatched();

    public abstract void execute(DBDatabase db, Statement statement) throws SQLException ;
    
}
