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
package nz.co.gregs.dbvolution.columns;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Represents the connection between a table and a column in a portable way.
 *
 * <p>
 * Used by
 * {@link RowDefinition#column(java.lang.Boolean) RowDefinition.getColumn(*)} to
 * produce an expression object that references a database table and column.
 *
 * <p>
 * Also allows PropertyWrapper to be passed around without confusing the public
 * interface.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author greg
 */
public class AbstractColumn implements DBExpression {

	private final PropertyWrapper propertyWrapper;
	private final RowDefinition dbrow;
	private final Object field;
	private boolean useTableAlias = true;

	/**
	 * Creates an AbstractColumn representing a table and column.
	 *
	 * <p>
	 * Stores the RowDefinition (generally a DBRow subclass) and a field of the
	 * RowDefinition so that the original association can be rebuilt where the
	 * expression is converted into SQL.
	 *
	 * @param row the row which contains the field
	 * @param field the field of the row that represents the database column
	 * @throws IncorrectRowProviderInstanceSuppliedException Please note the the
	 * field must be a field of the row
	 */
	public AbstractColumn(RowDefinition row, Object field) throws IncorrectRowProviderInstanceSuppliedException {
		this.dbrow = row;
		this.field = field;
		this.propertyWrapper = row.getPropertyWrapperOf(field);
		if (propertyWrapper == null) {
			throw IncorrectRowProviderInstanceSuppliedException.newMultiRowInstance(field);
		}
	}

	@Override
	public String toSQLString(DBDatabase db) {
		RowDefinition rowDefn = this.getRowDefinition();
		if ((field instanceof QueryableDatatype) && ((QueryableDatatype) field).hasColumnExpression()) {
			final QueryableDatatype qdtField = (QueryableDatatype) field;
			DBExpression[] columnExpressions = qdtField.getColumnExpression();
			String toSQLString="";
			for (DBExpression columnExpression : columnExpressions) {
				toSQLString += columnExpression.toSQLString(db);
			}
			return toSQLString;
		} else {
			String formattedName = "";
			if (useTableAlias) {
				formattedName = db.getDefinition().formatTableAliasAndColumnName(rowDefn, propertyWrapper.columnName());
			} else if (rowDefn instanceof DBRow) {
				DBRow dbRow = (DBRow) rowDefn;
				formattedName = db.getDefinition().formatTableAndColumnName(dbRow, propertyWrapper.columnName());
			}
			return propertyWrapper.getDefinition().getQueryableDatatype(dbrow).formatColumnForSQLStatementQuery(db, formattedName);
//        return "";
		}
	}

	@Override
	public AbstractColumn copy() {
		try {
			Constructor<? extends AbstractColumn> constructor = this.getClass().getConstructor(RowDefinition.class, Object.class);
			AbstractColumn newInstance = constructor.newInstance(getRowDefinition(), getField());
			return newInstance;
		} catch (NoSuchMethodException ex) {
			throw new DBRuntimeException("Unable To Copy " + this.getClass().getSimpleName() + ": please ensure it has a public " + this.getClass().getSimpleName() + "(DBRow, Object) constructor.", ex);
		} catch (SecurityException ex) {
			throw new DBRuntimeException("Unable To Copy " + this.getClass().getSimpleName() + ": please ensure it has a public " + this.getClass().getSimpleName() + "(DBRow, Object) constructor.", ex);
		} catch (InstantiationException ex) {
			throw new DBRuntimeException("Unable To Copy " + this.getClass().getSimpleName() + ": please ensure it has a public " + this.getClass().getSimpleName() + "(DBRow, Object) constructor.", ex);
		} catch (IllegalAccessException ex) {
			throw new DBRuntimeException("Unable To Copy " + this.getClass().getSimpleName() + ": please ensure it has a public " + this.getClass().getSimpleName() + "(DBRow, Object) constructor.", ex);
		} catch (IllegalArgumentException ex) {
			throw new DBRuntimeException("Unable To Copy " + this.getClass().getSimpleName() + ": please ensure it has a public " + this.getClass().getSimpleName() + "(DBRow, Object) constructor.", ex);
		} catch (InvocationTargetException ex) {
			throw new DBRuntimeException("Unable To Copy " + this.getClass().getSimpleName() + ": please ensure it has a public " + this.getClass().getSimpleName() + "(DBRow, Object) constructor.", ex);
		}
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the propertyWrapperOfQDT
	 */
	public PropertyWrapper getPropertyWrapper() {
		return propertyWrapper;
	}

	/**
	 * Wrap this column in the equivalent DBValue subclass.
	 *
	 * <p>
	 * Probably this should be implemented as:<br>
	 * public MyValue asExpression(){return new MyValue(this);}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this instance as a StringValue, NumberValue, DateValue, or
	 * LargeObjectValue as appropriate
	 */
	public DBExpression asExpression() {
		return this;
	}

	@Override
	public QueryableDatatype getQueryableDatatypeForExpressionValue() {
		return QueryableDatatype.getQueryableDatatypeForObject(getField());
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public boolean isPurelyFunctional() {
		return getTablesInvolved().size() == 0;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		HashSet<DBRow> hashSet = new HashSet<DBRow>();
		if (DBRow.class.isAssignableFrom(getRowDefinition().getClass())) {
			hashSet.add((DBRow) getRowDefinition());
		}
		return hashSet;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the dbrow
	 */
	protected RowDefinition getRowDefinition() {
		return dbrow;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the field
	 */
	protected Object getField() {
		return field;
	}

	/**
	 * Gets the DBvolution-centric value of the column for the instance of
	 * RowDefinition (DBRow/DBreport) supplied.
	 *
	 * <p>
	 * The value returned may have undergone type conversion from the target
	 * object's actual property type, if a type adaptor is present.
	 *
	 * @param row resolve the column for this row and provide the
	 * QueryableDatatype that is appropriate
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the QDT version of the field on the DBRow
	 */
	public QueryableDatatype getAppropriateQDTFromRow(RowDefinition row) {
		return this.getPropertyWrapper().getDefinition().getQueryableDatatype(row);
	}

	/**
	 * Gets the value of the declared column in the RowDefinition/DBRow/DBReport
	 * supplied, prior to type conversion to the DBvolution-centric type.
	 *
	 * <p>
	 * you should probably be using {@link #getAppropriateQDTFromRow(nz.co.gregs.dbvolution.query.RowDefinition)
	 * }
	 *
	 * @param row resolve the column for this row and provide the appropriate Java
	 * field (may be a QueryableDatatype)
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the actual field on the DBRow object referenced by this column.
	 */
	public Object getAppropriateFieldFromRow(RowDefinition row) {
		return this.getPropertyWrapper().getDefinition().rawJavaValue(row);
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the useTableAlias
	 */
	protected boolean isUseTableAlias() {
		return useTableAlias;
	}

	/**
	 * @param useTableAlias the useTableAlias to set
	 */
	protected void setUseTableAlias(boolean useTableAlias) {
		this.useTableAlias = useTableAlias;
	}

	/**
	 * Returns a new version of the DBRow from which this column has been made.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return an appropriate DBRow
	 */
	@SuppressWarnings("unchecked")
	public DBRow getInstanceOfRow() {
		final Class<? extends DBRow> originatingClass;
		originatingClass = (Class<? extends DBRow>) this.getPropertyWrapper().getRowDefinitionInstanceWrapper().adapteeRowDefinitionClass();
		final DBRow originatingRow = DBRow.getDBRow(originatingClass);
		return originatingRow;
	}

	/**
	 * Returns the class of the DBRow from which this column has been made.
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return an appropriate DBRow class
	 */
	public Class<? extends DBRow> getClassReferencedByForeignKey() {
		return this.getPropertyWrapper().referencedClass();
	}
}
