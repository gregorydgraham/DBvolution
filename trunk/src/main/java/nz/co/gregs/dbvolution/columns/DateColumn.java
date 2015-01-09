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
package nz.co.gregs.dbvolution.columns;

import java.util.Date;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Represents a database column storing a date value.
 *
 * <p>
 * This class adds the necessary methods to use a date column like a date
 * expression.
 *
 * <p>
 * Internally the class uses an AbsractColumn to store the column and overrides
 * methods in DateExpression to insert the column into the expression.
 *
 * <p>
 * Generally you get a DateColumn using
 * {@link RowDefinition#column(nz.co.gregs.dbvolution.datatypes.DBDate) RowDefinition.column(DBDate)}.
 *
 * @author Gregory Graham
 * @see RowDefinition
 * @see AbstractColumn
 * @see DateExpression
 */
public class DateColumn extends DateExpression implements ColumnProvider {

	private AbstractColumn column;

	private DateColumn() {
	}

	/**
	 * Create a DateColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public DateColumn(RowDefinition row, Date field) {
		this.column = new AbstractColumn(row, field);
	}


	/**
	 * Create a DateColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public DateColumn(RowDefinition row, DBDate field) {
		this.column = new AbstractColumn(row, field);
	}

	@Override
	public String toSQLString(DBDatabase db) {
		return column.toSQLString(db);
	}

	@Override
	public synchronized DateColumn copy() {
		try {
			DateColumn newInstance = this.getClass().newInstance();
			newInstance.column = this.column;
			return newInstance;
		} catch (InstantiationException ex) {
			throw new RuntimeException(ex);
		} catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		}

	}

	@Override
	public AbstractColumn getColumn() {
		return column;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return column.getTablesInvolved();
	}

	@Override
	public void setUseTableAlias(boolean useTableAlias) {
		this.column.setUseTableAlias(useTableAlias);
	}
}
