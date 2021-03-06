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
package nz.co.gregs.dbvolution.query;

import nz.co.gregs.dbvolution.results.StringResult;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPolygon2D;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.columns.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLine2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLineSegment2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBMultiPoint2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.*;
import nz.co.gregs.dbvolution.internal.properties.*;
import org.joda.time.Period;

/**
 * Encapsulates the concept of a row that has fields/columns and is part of a
 * table/view on a database.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class RowDefinition implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final RowDefinitionWrapperFactory wrapperFactory = new RowDefinitionWrapperFactory();
	private transient RowDefinitionInstanceWrapper wrapper = null;
	private transient List<PropertyWrapperDefinition> returnColumns = null;

	/**
	 * Gets a wrapper for the underlying property (field or method) given the
	 * property's object reference.
	 *
	 * <p>
	 * For example the following code snippet will get a property wrapper for the
	 * {@literal name} field:
	 * <pre>
	 * Customer customer = ...;
	 * getPropertyWrapperOf(customer.name);
	 * </pre>
	 *
	 * @param qdt	qdt
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the PropertyWrapper associated with the Object suppled or NULL.
	 */
	public PropertyWrapper getPropertyWrapperOf(Object qdt) {
		List<PropertyWrapper> props = getWrapper().getColumnPropertyWrappers();

		Object qdtOfProp;
		for (PropertyWrapper prop : props) {
			qdtOfProp = prop.rawJavaValue();
			if (qdtOfProp == qdt) {
				return prop;
			}
		}
		return null;
	}

	/**
	 * Return the {@link RowDefinitionInstanceWrapper } for this RowDefinition.
	 *
	 * <p>
	 * The {@link RowDefinitionInstanceWrapper } contains meta-data about this
	 * instance of the RowDefinition class.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a {@link RowDefinitionInstanceWrapper}
	 */
	protected RowDefinitionInstanceWrapper getWrapper() {
		if (wrapper == null) {
			wrapper = wrapperFactory.instanceWrapperFor(this);
		}
		return wrapper;
	}

	/**
	 * Returns the PropertyWrappers used internally to maintain the relationship
	 * between fields and columns
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return non-null list of property wrappers, empty if none
	 */
	public List<PropertyWrapper> getColumnPropertyWrappers() {
		return getWrapper().getColumnPropertyWrappers();
	}

	/**
	 * Returns the PropertyWrappers of normal non-column properties.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return non-null list of property wrappers, empty if none
	 */
	public List<PropertyWrapper> getAutoFillingPropertyWrappers() {
		return getWrapper().getAutoFillingPropertyWrappers();
	}

	/**
	 * Creates a new LargeObjectColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A LargeObjectColumn representing the supplied field
	 */
	public LargeObjectColumn column(DBLargeObject fieldOfThisInstance) {
		return new LargeObjectColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new {@link DateRepeatColumn} instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A DateRepeatColumn representing the supplied field
	 */
	public DateRepeatColumn column(DBDateRepeat fieldOfThisInstance) {
		return new DateRepeatColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new {@link BooleanArrayColumn} instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A {@link BooleanArrayColumn} representing the supplied field
	 */
	public BooleanArrayColumn column(DBBooleanArray fieldOfThisInstance) {
		return new BooleanArrayColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new {@link Polygon2DColumn} instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A {@link Polygon2DColumn} representing the supplied field
	 */
	public Polygon2DColumn column(DBPolygon2D fieldOfThisInstance) {
		return new Polygon2DColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new {@link Point2DColumn} instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A {@link Point2DColumn} representing the supplied field
	 */
	public Point2DColumn column(DBPoint2D fieldOfThisInstance) {
		return new Point2DColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new {@link Line2DColumn} instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A {@link Line2DColumn} representing the supplied field
	 */
	public Line2DColumn column(DBLine2D fieldOfThisInstance) {
		return new Line2DColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new {@link LineSegment2DColumn} instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A {@link LineSegment2DColumn} representing the supplied field
	 */
	public LineSegment2DColumn column(DBLineSegment2D fieldOfThisInstance) {
		return new LineSegment2DColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new {@link MultiPoint2DColumn} instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A {@link MultiPoint2DColumn} representing the supplied field
	 */
	public MultiPoint2DColumn column(DBMultiPoint2D fieldOfThisInstance) {
		return new MultiPoint2DColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new ColumnProvider instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A ColumnProvider representing the supplied field
	 */
	public ColumnProvider column(QueryableDatatype fieldOfThisInstance) throws IncorrectRowProviderInstanceSuppliedException {
		ColumnProvider col = null;
		if (DBBoolean.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBBoolean) fieldOfThisInstance);
		} else if (DBDate.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBDate) fieldOfThisInstance);
		} else if (DBLargeObject.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBLargeObject) fieldOfThisInstance);
		} else if (DBInteger.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBInteger) fieldOfThisInstance);
		} else if (DBIntegerEnum.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBIntegerEnum) fieldOfThisInstance);
		} else if (DBNumber.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBNumber) fieldOfThisInstance);
		} else if (DBStringEnum.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBStringEnum) fieldOfThisInstance);
		} else if (DBString.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBString) fieldOfThisInstance);
		} else if (DBDateRepeat.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBDateRepeat) fieldOfThisInstance);
		} else if (DBBooleanArray.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBBooleanArray) fieldOfThisInstance);
		} else if (DBPolygon2D.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBPolygon2D) fieldOfThisInstance);
		} else if (DBPoint2D.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBPoint2D) fieldOfThisInstance);
		} else if (DBLine2D.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBLine2D) fieldOfThisInstance);
		} else if (DBLineSegment2D.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBLineSegment2D) fieldOfThisInstance);
		} else if (DBMultiPoint2D.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBMultiPoint2D) fieldOfThisInstance);
		} else if (DBNumberStatistics.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBNumber) fieldOfThisInstance);
		}
		if (col == null) {
			throw new IncorrectRowProviderInstanceSuppliedException(this, fieldOfThisInstance);
		}
		return col;
	}

	/**
	 * Creates a new ColumnProvider instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A ColumnProvider representing the supplied field
	 */
	public ColumnProvider column(Object fieldOfThisInstance) throws IncorrectRowProviderInstanceSuppliedException {
		ColumnProvider col = null;
		if (QueryableDatatype.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((QueryableDatatype) fieldOfThisInstance);
		} else if (Period.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((Period) fieldOfThisInstance);
		} else if (Date.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((Date) fieldOfThisInstance);
		} else if (Boolean.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((Boolean) fieldOfThisInstance);
		} else if (Integer.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((Integer) fieldOfThisInstance);
		} else if (Number.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((Number) fieldOfThisInstance);
		} else if (String.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((String) fieldOfThisInstance);
		} else {
			throw new UnsupportedOperationException("Object class not supported: " + fieldOfThisInstance.getClass().getName());
		}
		if (col == null) {
			throw new IncorrectRowProviderInstanceSuppliedException(this, fieldOfThisInstance);
		}
		return col;
	}

	/**
	 * Creates a new DBExpression instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A DBExpression representing the supplied field
	 */
	public DBExpression getDBExpression(QueryableDatatype fieldOfThisInstance) throws IncorrectRowProviderInstanceSuppliedException {
		DBExpression col = null;
		if (DBBoolean.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBBoolean) fieldOfThisInstance);
		} else if (DBDate.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBDate) fieldOfThisInstance);
		} else if (DBLargeObject.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBLargeObject) fieldOfThisInstance);
		} else if (DBInteger.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBInteger) fieldOfThisInstance);
		} else if (DBNumber.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBNumber) fieldOfThisInstance);
		} else if (DBString.class.isAssignableFrom(fieldOfThisInstance.getClass())) {
			col = this.column((DBString) fieldOfThisInstance);
		}
		if (col == null) {
			throw new IncorrectRowProviderInstanceSuppliedException(this, fieldOfThisInstance);
		}
		return col;
	}

	/**
	 * Creates a new BooleanColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A LargeObjectColumn representing the supplied field
	 */
	public BooleanColumn column(DBBoolean fieldOfThisInstance) {
		return new BooleanColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new BooleanColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A Column representing the supplied field
	 */
	public BooleanColumn column(Boolean fieldOfThisInstance) {
		return new BooleanColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new StringColumn instance to help create
	 * {@link DBExpression expressions}.
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A Column representing the supplied field
	 */
	public StringColumn column(DBStringEnum<?> fieldOfThisInstance) {
		return new StringColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new StringColumn instance to help create
	 * {@link DBExpression expressions}.
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A Column representing the supplied field
	 */
	public StringColumn column(DBString fieldOfThisInstance) {
		return new StringColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new StringColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A Column representing the supplied field
	 */
	public StringColumn column(String fieldOfThisInstance) {
		return new StringColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new StringExpression for the field with a default value for NULL
	 * entries.
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A StringExpression representing the supplied field with a default
	 * value for NULLs
	 */
	public StringExpression column(DBString fieldOfThisInstance, StringResult valueToUseIfDBNull) {
		return (new StringColumn(this, fieldOfThisInstance)).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new StringExpression for the field with a default value for NULL
	 * entries.
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A StringExpression representing the supplied field with a default
	 * value for NULLs
	 */
	public StringExpression column(DBString fieldOfThisInstance, String valueToUseIfDBNull) {
		return (new StringColumn(this, fieldOfThisInstance)).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new StringExpression for the field with a default value for NULL
	 * entries.
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A StringExpression representing the supplied field with a default
	 * value for NULLs
	 */
	public StringExpression column(String fieldOfThisInstance, StringResult valueToUseIfDBNull) {
		return (new StringColumn(this, fieldOfThisInstance)).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new StringExpression for the field with a default value for NULL
	 * entries.
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A StringExpression representing the supplied field with a default
	 * value for NULLs
	 */
	public StringExpression column(String fieldOfThisInstance, String valueToUseIfDBNull) {
		return (new StringColumn(this, fieldOfThisInstance)).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A Column representing the supplied field
	 */
	public NumberColumn column(DBNumber fieldOfThisInstance) {
		return new NumberColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new NumberColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A Column representing the supplied field
	 */
	public NumberColumn column(Number fieldOfThisInstance) {
		return new NumberColumn(this, fieldOfThisInstance);
	}
	/**
	 * Creates a new DateRepeatColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A Column representing the supplied field
	 */
	public DateRepeatColumn column(Period fieldOfThisInstance) {
		return new DateRepeatColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(Number fieldOfThisInstance, Number valueToUseIfDBNull) {
		return new NumberColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(DBNumber fieldOfThisInstance, Number valueToUseIfDBNull) {
		return new NumberColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(DBNumber fieldOfThisInstance, NumberResult valueToUseIfDBNull) {
		return new NumberColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new IntegerColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A Column representing the supplied field
	 */
	public IntegerColumn column(DBInteger fieldOfThisInstance) {
		return new IntegerColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new IntegerColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A Column representing the supplied field
	 */
	public IntegerColumn column(DBIntegerEnum<?> fieldOfThisInstance) {
		return new IntegerColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new IntegerColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A Column representing the supplied field
	 */
	public IntegerColumn column(Long fieldOfThisInstance) {
		return new IntegerColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new IntegerColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A Column representing the supplied field
	 */
	public IntegerColumn column(Integer fieldOfThisInstance) {
		return new IntegerColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(Integer fieldOfThisInstance, Integer valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(Integer fieldOfThisInstance, Long valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(Integer fieldOfThisInstance, NumberResult valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(DBInteger fieldOfThisInstance, Integer valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(DBInteger fieldOfThisInstance, Long valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(DBInteger fieldOfThisInstance, NumberResult valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(Long fieldOfThisInstance, Integer valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(Long fieldOfThisInstance, Long valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new NumberExpression representing the column and supplying a
	 * default value for NULL entries.
	 *
	 * <p>
	 * This method is an easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance fieldOfThisInstance
	 * @param valueToUseIfDBNull valueToUseIfDBNull
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A NumberExpression representing the supplied field with a default
	 * for DBNULL values
	 */
	public NumberExpression column(Long fieldOfThisInstance, NumberResult valueToUseIfDBNull) {
		return new IntegerColumn(this, fieldOfThisInstance).ifDBNull(valueToUseIfDBNull);
	}

	/**
	 * Creates a new DateColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A Column representing the supplied field
	 */
	public DateColumn column(DBDate fieldOfThisInstance) {
		return new DateColumn(this, fieldOfThisInstance);
	}

	/**
	 * Creates a new DateColumn instance to help create
	 * {@link DBExpression expressions}
	 *
	 * <p>
	 * This method is the easy way to create a reference to the database column
	 * represented by the field for use in creating complex expressions within
	 * your query.
	 *
	 * <p>
	 * For use with the
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) DBQuery addCondition method}
	 *
	 * @param fieldOfThisInstance	fieldOfThisInstance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A Column representing the supplied field
	 */
	public DateColumn column(Date fieldOfThisInstance) {
		return new DateColumn(this, fieldOfThisInstance);
	}

	@Override
	public String toString() {
		StringBuilder string = new StringBuilder();
		List<PropertyWrapper> fields = getWrapper().getColumnPropertyWrappers();

		String separator = "" + this.getClass().getSimpleName();

		for (PropertyWrapper field : fields) {
			if (field.isColumn()) {
				string.append(separator);
				string.append(" ");
				string.append(field.javaName());
				string.append(":");
				string.append(field.getQueryableDatatype());
				separator = ",";
			}
		}
		return string.toString();
	}

	/**
	 * Returns all the fields names from all fields in this DBRow.
	 *
	 * <p>
	 * This is essentially a list of all the columns returned from the database
	 * query.
	 *
	 * <p>
	 * Column data may not have been populated.
	 *
	 * <p>
	 * Please note this is a crude instrument for accessing the data in this
	 * DBRow. You should probably be using the fields and methods of the actual
	 * DBRow class.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of field names.
	 *
	 */
	public List<String> getFieldNames() {
		List<String> returnList = new ArrayList<String>();
		List<PropertyWrapperDefinition> fieldDefns = this.getReturnColumns();
		for (PropertyWrapperDefinition prop : fieldDefns) {
			returnList.add(prop.javaName());
		}
		return returnList;
	}

	/**
	 * Returns all the fields values from this DBRow.
	 *
	 * <p>
	 * This is essentially a list of all the values returned from the database
	 * query.
	 *
	 * <p>
	 * Please note this is a crude instrument for accessing the data in this
	 * DBRow. You should probably be using the fields and methods of the DBRow
	 * class.
	 *
	 * @param dateFormat	dateFormat
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of field names.
	 *
	 */
	public Collection<String> getFieldValues(SimpleDateFormat dateFormat) {
		List<String> returnList = new ArrayList<String>();
		for (PropertyWrapperDefinition prop : this.getReturnColumns()) {
			QueryableDatatype qdt = prop.getQueryableDatatype(this);
			if (dateFormat != null && DBDate.class.isAssignableFrom(qdt.getClass())) {
				DBDate dBDate = (DBDate) qdt;
				Date dateValue = dBDate.dateValue();
				if (dateValue != null) {
					returnList.add(dateFormat.format(dateValue));
				} else {
					returnList.add(qdt.stringValue());
				}
			} else {
				returnList.add(qdt.stringValue());
			}
		}
		return returnList;
	}

	/**
	 * Returns a list of the columns that are set to be return during a query.
	 *
	 * <p>
	 * By default this list is all the columns in the RowDefinition.
	 * <p>
	 * However it can be changed using {@link #setReturnColumns(java.util.List)
	 * }.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of the columns to be selected and returned in the query.
	 */
	protected List<PropertyWrapperDefinition> getReturnColumns() {
		if (returnColumns == null) {
			returnColumns = this.getColumnPropertyWrapperDefinitions();
		}
		return returnColumns;
	}

	/**
	 * Changed the list of columns that are to be return during a query.
	 *
	 * @param returnColumns
	 */
	protected void setReturnColumns(List<PropertyWrapperDefinition> returnColumns) {
		this.returnColumns = returnColumns;
	}

	/**
	 * Return the list of
	 * {@link PropertyWrapperDefinition PropertyWrapperDefinitions} for all the
	 * columns within this RowDefinition.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of the PropertyWrapperDefinition for columns.
	 */
	protected List<PropertyWrapperDefinition> getColumnPropertyWrapperDefinitions() {
		List<PropertyWrapperDefinition> columns = new ArrayList<PropertyWrapperDefinition>();
		List<PropertyWrapper> propertyWrappers = this.getColumnPropertyWrappers();
		for (PropertyWrapper propertyWrapper : propertyWrappers) {
			columns.add(propertyWrapper.getDefinition());
		}
		return columns;
	}


}
