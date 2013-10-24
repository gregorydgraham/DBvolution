package nz.co.gregs.dbvolution.internal;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.DBPebkacException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;

/**
 * Abstracts a java field or bean-property as a DBvolution-centric
 * property, which contains values from a specific column in a database table.
 * Transparently handles all annotations associated with the property,
 * including type adaption.
 * 
 * <p> Provides access to the meta-data defined on a single java property of a class,
 * and provides methods for reading and writing the value of the property
 * on target objects.
 * Instances of this class are not bound to specific target objects, nor are they
 * bound to specific database definitions.
 * 
 * <p> For binding to specific target objects and database definitions,
 * use the {@link DBProperty} class.
 * 
 * <p> DB properties can be seen to have the types and values in the table that follows.
 * This class provides a virtual view over the property whereby the DBv-centric type
 * and value are easily accessible via the {@link #getQueryableDatatype(Object) value()} and
 * {@link #setQueryableDatatype(Object, QueryableDatatype) setValue()} methods.
 * <ul>
 * <li> rawType/rawValue - the type and value actually stored on the declared java property
 * <li> dbvType/dbvValue - the type and value used within DBv (a QueryableDataType)
 * <li> databaseType/databaseValue - the type and value of the database column itself (this class doesn't deal with these) 
 * </ul>
 * 
 * <p> Note: instances of this class are expensive to create and should be cached.
 * 
 * <p> This class is <i>thread-safe</i>.
 */
public class PropertyWrapperDefinition {
	private final JavaProperty adaptee;
	
	private final ColumnHandler columnHandler;
	private final PropertyTypeHandler typeHandler;
	private final ForeignKeyHandler foreignKeyHandler;
	
	public PropertyWrapperDefinition(JavaProperty javaProperty) {
		this.adaptee = javaProperty;
		
		// handlers
		this.columnHandler = new ColumnHandler(javaProperty);
		this.typeHandler = new PropertyTypeHandler(javaProperty);
		this.foreignKeyHandler = new ForeignKeyHandler(javaProperty);
	}
	
	/**
	 * Gets a string representation of the wrapped property,
	 * suitable for debugging and logging.
	 */
	@Override
	public String toString() {
		return adaptee.toString();
	}

	/**
	 * Generates a hash-code of this property wrapper definition,
	 * based entirely on the java property it wraps.
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((adaptee == null) ? 0 : adaptee.hashCode());
		return result;
	}

	/**
	 * Equality of this property wrapper definition,
	 * based on the java property it wraps in a specific class.
	 * Two instances are identical if they wrap the same java property (field or bean-property)
	 * in the same class and the same class-loader.
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof PropertyWrapperDefinition)) {
			return false;
		}
		PropertyWrapperDefinition other = (PropertyWrapperDefinition) obj;
		if (adaptee == null) {
			if (other.adaptee != null) {
				return false;
			}
		} else if (!adaptee.equals(other.adaptee)) {
			return false;
		}
		return true;
	}

	/**
	 * Gets the name of the underlying java property.
	 * Mainly used within logging and error messages.
	 * 
	 * <p> Use {@link #columnName()} to determine column name.
	 * @return
	 */
	public String javaName() {
		return adaptee.name();
	}
	
	/**
	 * Gets the qualified name of the underlying java property.
	 * Mainly used within logging and error messages.
	 * @return
	 */
	public String qualifiedJavaName() {
		return adaptee.qualifiedName();
	}

	/**
	 * Gets the DBvolution-centric type of the property.
	 * If a type adaptor is present, then this is the type after conversion
	 * from the target object's actual property type.
	 * 
	 * <p> Use {@link #getRawJavaType()} in the rare case that you need to know the underlying
	 * java property type.
	 * @return
	 */
	public Class<? extends QueryableDatatype> type() {
		return typeHandler.getType();
	}
	
	/**
	 * Gets the annotated column name.
	 * Applies defaulting if the {@code DBColumn} annotation is present
	 * but does not explicitly specify the column name.
	 * 
	 * <p> If the {@code DBColumn} annotation is missing, this method returns {@code null}.
	 * 
	 * <p> Use {@link #getDBColumnAnnotation} for low level access.
	 * @return the column name, if specified explicitly or implicitly
	 */
	public String columnName() {
		return columnHandler.getColumnName();
	}

	/**
	 * Indicates whether this property is a column.
	 * @return {@code true} if this property is a column
	 */
	public boolean isColumn() {
		return columnHandler.isColumn();
	}
	
	/**
	 * Indicates whether this property is a primary key.
	 * @return {@code true} if this property is a primary key
	 */
	public boolean isPrimaryKey() {
		return columnHandler.isPrimaryKey();
	}
	
	/**
	 * Indicates whether this property is a foreign key.
	 * @return {@code true} if this property is a foreign key
	 */
	public boolean isForeignKey() {
		return foreignKeyHandler.isForeignKey();
	}

	/**
	 * Gets the class referenced by this property, if this property
	 * is a foreign key.
	 * @return the referenced class or null if not applicable
	 */
	public Class<? extends DBRow> referencedClass() {
		return foreignKeyHandler.getReferencedClass();
	}
	
	/**
	 * Gets the table referenced by this property, if this property
	 * is a foreign key.
	 * @return the referenced table name, or null if not applicable
	 */
	public String referencedTableName() {
		return foreignKeyHandler.getReferencedTableName();
	}
	
	/**
	 * Gets the column name in the foreign table referenced by this property,
	 * if this property is a foreign key.
	 * Referenced column names may not be specified, in which case the foreign key
	 * references the primary key in the foreign class/table.
	 * 
	 * <p> Use {@link #getDBForeignKeyAnnotation} for low level access.
	 * @param database the current active database
	 * @param cache the class wrapper factory
	 * @return the referenced column name, or null if not specified or not applicable
	 */
	// TODO update javadoc for this method now that it's got more smarts
	public String referencedColumnName(DBDatabase database, DBRowWrapperFactory cache) {
		PropertyWrapperDefinition referencedProperty = referencedProperty(database, cache);
		if (referencedProperty != null) {
			return referencedProperty.columnName();
		}
		return null;
	}

	/**
	 * Note: this returns only a single property; in the case where multiple foreign key
	 * columns are used together to reference a table with a composite primary key,
	 * each foreign key column references its respective foreign primary key.
	 * @param database the current active database
	 * @param cache the active class adaptor cache
	 * @return the mapped foreign key property, or null if not a foreign key
	 * @throws DBPebkacException if the foreign table has multiple primary keys and the foreign key
	 *         column doesn't identify which primary key column to target
	 */
	// An idea of what could be possible; to be decided whether we want to keep this
	public PropertyWrapperDefinition referencedProperty(DBDatabase database, DBRowWrapperFactory cache) {
		if (!foreignKeyHandler.isForeignKey()) {
			return null;
		}
		if (foreignKeyHandler.getReferencedClass() == null) {
			// sanity check
			throw new DBRuntimeException("Unexpected internal error: referenced class was null on "+qualifiedJavaName());
		}

		DBRowClassWrapper referencedClassAdaptor = cache.classWrapperFor(foreignKeyHandler.getReferencedClass());
		
		// get explicitly referenced property (by column name)
		String explicitColumnName = foreignKeyHandler.getReferencedColumnName();
		if (explicitColumnName != null) {
			PropertyWrapperDefinition property = referencedClassAdaptor.getPropertyDefinitionByColumn(database, explicitColumnName);
			if (property == null) {
				// TODO do this validation at annotation processing time?
				throw new DBPebkacException("Property "+qualifiedJavaName()+" references class "+referencedClassAdaptor.javaName()
						+" and column "+explicitColumnName+", but the column doesn't exist");
			}
			return property;
		}
		
		// get implicitly referenced property (by scalar primary key)
		else {
			PropertyWrapperDefinition primaryKeyProperties = referencedClassAdaptor.primaryKeyDefinition();
			if (primaryKeyProperties == null) {
				// TODO do this validation at annotation processing time
				// TODO not sure if it's appropriate to throw this exception here
				throw new DBPebkacException("Property "+qualifiedJavaName()+" references class "+referencedClassAdaptor.javaName()
						+", which does not have a primary key");
			}
			else {
				// TODO do this validation at annotation processing time
//				throw new DBPebkacException("Property "+qualifiedJavaName()+" references class "+referencedClassAdaptor.javaName()
//						+" using an implicit primary key reference, but the referenced class has "+primaryKeyProperties.size()
//						+" primary key columns. You must include explicit foreign column names.");
			}
			
			return primaryKeyProperties;
		}
	}

	/**
	 * Gets the column name in the foreign table referenced by this property,
	 * if this property is a foreign key.
	 * Referenced column names may not be specified, in which case the foreign key
	 * references the primary key in the foreign class/table.
	 * 
	 * <p> Use {@link #getDBForeignKeyAnnotation} for low level access.
	 * @return the referenced column name, or null if not specified or not applicable
	 */
	// TODO improve javadoc
	public String declaredReferencedColumnName() {
		return foreignKeyHandler.getReferencedColumnName();
	}
	
	/**
	 * Indicates whether the value of the property can be retrieved.
	 * Bean properties which are missing a 'getter' can not be read,
	 * but may be able to be set.
	 * @return
	 */
	public boolean isReadable() {
		return adaptee.isReadable();
	}

	/**
	 * Indicates whether the value of the property can be modified.
	 * Bean properties which are missing a 'setter' can not be written to,
	 * but may be able to be read.
	 * @return
	 */
	public boolean isWritable() {
		return adaptee.isWritable();
	}

	/**
	 * Gets the DBvolution-centric value of the property.
	 * The value returned may have undergone type conversion from the target object's
	 * actual property type, if a type adaptor is present.
	 * 
	 * <p> Use {@link #isReadable()} beforehand to check whether the property
	 * can be read.
	 * @param target object instance containing this property
	 * @return
	 * @throws IllegalStateException if not readable (you should have called isReadable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public QueryableDatatype getQueryableDatatype(Object target) {
		return typeHandler.getDBvValue(target);
	}
	
	/**
	 * Sets the DBvolution-centric value of the property.
	 * The value set may have undergone type conversion to the target object's
	 * actual property type, if a type adaptor is present.
	 * 
	 * <p> Use {@link #isWritable()} beforehand to check whether the property
	 * can be modified.
	 * @param target object instance containing this property
	 * @param value
	 * @throws IllegalStateException if not writable (you should have called isWritable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public void setQueryableDatatype(Object target, QueryableDatatype value) {
		typeHandler.setObjectValue(target, value);
	}
	
	/**
	 * Gets the value of the declared property in the end-user's target object,
	 * prior to type conversion to the DBvolution-centric type.
	 * 
	 * <p> In most cases you will not need to call this method, as type
	 * conversion is done transparently via the {@link #getQueryableDatatype(Object)} and
	 * {@link #setQueryableDatatype(Object, QueryableDatatype)} methods.
	 * 
	 * <p> Use {@link #isReadable()} beforehand to check whether the property
	 * can be read.
	 * @param target object instance containing this property
	 * @return value
	 * @throws IllegalStateException if not readable (you should have called isReadable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public Object rawJavaValue(Object target) {
		return adaptee.get(target);
	}
	
	/**
	 * Set the value of the declared property in the end-user's target object,
	 * without type conversion to/from the DBvolution-centric type.
	 * 
	 * <p> In most cases you will not need to call this method, as type
	 * conversion is done transparently via the {@link #getQueryableDatatype(Object)} and
	 * {@link #setQueryableDatatype(Object, QueryableDatatype)} methods.
	 * 
	 * <p> Use {@link #isWritable()} beforehand to check whether the property
	 * can be modified.
	 * @param target object instance containing this property
	 * @param value new value
	 * @throws IllegalStateException if not writable (you should have called isWritable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public void setRawJavaValue(Object target, Object value) {
		adaptee.set(target, value);
	}
	
	/**
	 * Gets the declared type of the property in the end-user's target object,
	 * prior to type conversion to the DBvolution-centric type.
	 * 
	 * <p> In most cases you will not need to call this method, as type
	 * conversion is done transparently via the {@link #getQueryableDatatype(Object)} and
	 * {@link #setQueryableDatatype(Object, QueryableDatatype)} methods.
	 * Use the {@link #type()} method to get the DBv-centric property type,
	 * after type conversion.
	 * @return
	 */
	public Class<?> getRawJavaType() {
		return adaptee.type();
	}
	
	// commented out because shouldn't be needed:
//		/**
//		 * Gets the {@link DBColumn} annotation on the property, if it exists.
//		 * @return the annotation or null
//		 */
//		public DBColumn getDBColumnAnnotation() {
//			return columnHandler.getDBColumnAnnotation();
//		}

	// commented out because shouldn't be needed:
//		/**
//		 * Gets the {@link DBForeignKey} annotation on the property, if it exists.
//		 * @return the annotation or null
//		 */
//		public DBForeignKey getDBForeignKeyAnnotation() {
//			return foreignKeyHandler.getDBForeignKeyAnnotation();
//		}
		
	// commented out because shouldn't be needed:
//		/**
//		 * Gets the {@link DBTypeAdaptor} annotation on the property, if it exists.
//		 * @return the annotation or null
//		 */
//		public DBAdaptType getDBTypeAdaptorAnnotation() {
//			return typeHandler.getDBTypeAdaptorAnnotation();
//		}
}
