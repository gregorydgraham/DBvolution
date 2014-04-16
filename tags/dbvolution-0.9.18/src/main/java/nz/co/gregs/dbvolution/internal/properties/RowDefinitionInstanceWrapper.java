package nz.co.gregs.dbvolution.internal.properties;

import java.util.ArrayList;
import java.util.List;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Wraps a specific target object according to its type's
 * {@link RowDefinitionClassWrapper}.
 *
 * <p>
 * To create instances of this type, call
 * {@link RowDefinitionWrapperFactory#instanceWrapperFor(nz.co.gregs.dbvolution.query.RowDefinition)}
 * on the appropriate {@link RowDefinition}.
 *
 * <p>
 * Instances of this class are lightweight and efficient to create, and they are
 * intended to be short lived. Instances of this class must not be shared
 * between different DBDatabase instances, however they can be safely associated
 * within a single DBDatabase instance.
 *
 * <p>
 * Instances of this class are <i>thread-safe</i>.
 *
 * @author Malcolm Lett
 */
public class RowDefinitionInstanceWrapper {

    private final RowDefinitionClassWrapper classWrapper;
    private final RowDefinition rowDefinition;
    private final List<PropertyWrapper> allProperties;
    private final List<PropertyWrapper> foreignKeyProperties;

    /**
     * Called by
     * {@link DBRowClassWrapper#instanceAdaptorFor(DBDefinition, Object)}.
     *
     * @param classWrapper
     * @param rowDefinition the target object of the same type as analyzed by {@code classWrapper}
     */
    RowDefinitionInstanceWrapper(RowDefinitionClassWrapper classWrapper, RowDefinition rowDefinition) {
        if (rowDefinition == null) {
            throw new DBRuntimeException("Target object is null");
        }
        if (!classWrapper.adapteeClass().isInstance(rowDefinition)) {
            throw new DBRuntimeException("Target object's type (" + rowDefinition.getClass().getName()
                    + ") is not compatible with given class adaptor for type " + classWrapper.adapteeClass().getName()
                    + " (this is probably a bug in DBvolution)");
        }

        this.rowDefinition = rowDefinition;
        this.classWrapper = classWrapper;

        // pre-cache commonly used things
        // (note: if you change this to use lazy-initialisation, you'll have to
        // add explicit synchronisation, or it won't be thread-safe anymore)
        this.allProperties = new ArrayList<PropertyWrapper>();
        for (PropertyWrapperDefinition propertyDefinition : classWrapper.getPropertyDefinitions()) {
            this.allProperties.add(new PropertyWrapper(this, propertyDefinition, rowDefinition));
        }

        this.foreignKeyProperties = new ArrayList<PropertyWrapper>();
        for (PropertyWrapperDefinition propertyDefinition : classWrapper.getForeignKeyPropertyDefinitions()) {
            this.foreignKeyProperties.add(new PropertyWrapper(this, propertyDefinition, rowDefinition));
        }
    }

    /**
     * Gets a string representation suitable for debugging.
     *
     * @return a String representing this object sufficient for debugging
     * purposes
     */
    @Override
    public String toString() {
        if (isTable()) {
            return getClass().getSimpleName() + "<" + tableName() + ":" + classWrapper.adapteeClass().getName() + ">";
        } else {
            return getClass().getSimpleName() + "<no-table:" + classWrapper.adapteeClass().getName() + ">";
        }
    }
    
	/**
	 * Two {@code RowDefinitionInstanceWrappers} are equal if they wrap two {@code RowDefinition} instances
	 * that are themselves equal, and are instances of the same class.
	 * @return {@code true} if the two objects are equal, {@code false} otherwise.
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof RowDefinitionInstanceWrapper)) {
			return false;
		}
		RowDefinitionInstanceWrapper other = (RowDefinitionInstanceWrapper) obj;
		if (classWrapper == null) {
			if (other.classWrapper != null) {
				return false;
			}
		} else if (!classWrapper.equals(other.classWrapper)) {
			return false;
		}
		if (rowDefinition == null) {
			if (other.rowDefinition != null) {
				return false;
			}
		} else if (!rowDefinition.equals(other.rowDefinition)) {
			return false;
		}
		return true;
	}

	/**
	 * Calculates the hash-code based on the hash-code of the wrapped @{code RowDefinition}
	 * instance and its class.
	 * @return the hash-code
	 */
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((classWrapper == null) ? 0 : classWrapper.hashCode());
		result = prime * result + ((rowDefinition == null) ? 0 : rowDefinition.hashCode());
		return result;
	}
	
    /**
     * Gets the class-wrapper for the class of wrapped {@code RowDefinition}
     * @return the class-wrapper
     */
	public RowDefinitionClassWrapper getClassWrapper() {
        return classWrapper;
    }

    /**
     * Gets the wrapped object type supported by this {@code ObjectAdaptor}.
     * Note: this should be the same as the wrapped object's actual type.
     *
     * @return the class of the wrapped instance
     */
    public Class<?> adapteeRowDefinitionClass() {
        return classWrapper.adapteeClass();
    }

    /**
     * Gets the {@link RowDefinition} instance wrapped by this
     * {@code ObjectAdaptor}.
     *
     * @return the {@link RowDefinition} (usually a {@link DBRow} or
     * {@link DBReport}) for this instance.
     */
    public RowDefinition adapteeRowDefinition() {
        return rowDefinition;
    }

    /**
     * Gets the simple name of the class being wrapped by this adaptor.
     * <p>
     * Use {@link #tableName()} for the name of the table mapped to this class.
     *
     * @return the simple class name of the wrapped RowDefinition
     */
    public String javaName() {
        return classWrapper.javaName();
    }

    /**
     * Gets the fully qualified name of the class being wrapped by this adaptor.
     * <p>
     * Use {@link #tableName()} for the name of the table mapped to this class.
     *
     * @return the full class name of the wrapped RowDefinition
     */
    public String qualifiedJavaName() {
        return classWrapper.qualifiedJavaName();
    }

    /**
     * Indicates whether this class maps to a database table.
     *
     * <p>
     * If the wrapped {@link RowDefinition} is a {@link DBRow} and thus maps
     * directly to a table or view, this method returns true. Other
     * RowDefinitions, probably {@link DBReport}, will return false.
     *
     * @return TRUE if this RowDefinition maps directly to a table or view,
     * FALSE otherwise
     */
    public boolean isTable() {
        return classWrapper.isTable();
    }

    /**
     * Gets the indicated table name. Applies defaulting if the
     * {@link DBTableName} annotation is present but doesn't provide an explicit
     * table name.
     *
     * <p>
     * If the {@link DBTableName} annotation is missing, this method returns
     * {@code null}.
     *
     * @return the table name, if specified explicitly or implicitly.
     */
    public String tableName() {
        return classWrapper.tableName();
    }

    /**
     * Gets the property that is the primary key, if one is marked. Note:
     * multi-column primary key tables are not yet supported.
     *
     * @return the primary key property or null if no primary key
     */
    public PropertyWrapper primaryKey() {
        if (classWrapper.primaryKeyDefinition() != null) {
            return new PropertyWrapper(this, classWrapper.primaryKeyDefinition(), rowDefinition);
        } else {
            return null;
        }
    }

    /**
     * Gets the property associated with the given column.
     *
     * <p>
     * If multiple properties are annotated for the same column, this method
     * will return only the first.
     *
     * <p>
     * Only provides access to properties annotated with {@code DBColumn}.
     *
     * <p>
     * Assumes validation is applied elsewhere to prohibit duplication of column
     * names.
     *
     * @param database
     * @param columnName
     * @return the Java property associated with the column name supplied
     */
    public PropertyWrapper getPropertyByColumn(DBDatabase database, String columnName) {
        PropertyWrapperDefinition classProperty = classWrapper.getPropertyDefinitionByColumn(database, columnName);
        return (classProperty == null) ? null : new PropertyWrapper(this, classProperty, rowDefinition);
    }

    /**
     * Gets the property by its java field name.
     * <p>
     * Only provides access to properties annotated with {@code DBColumn}.
     *
     * @param propertyName
     * @return property of the wrapped {@link RowDefinition} associated with the java field name supplied
     */
    public PropertyWrapper getPropertyByName(String propertyName) {
        PropertyWrapperDefinition classProperty = classWrapper.getPropertyDefinitionByName(propertyName);
        return (classProperty == null) ? null : new PropertyWrapper(this, classProperty, rowDefinition);
    }

    /**
     * Gets all properties that are annotated with {@code DBColumn}. This method
     * is intended for where you need to get/set property values on all
     * properties in the class.
     *
     * <p>
     * Note: if you wish to iterate over the properties and only use their
     * definitions (ie: meta-information), this method is not efficient. Use
     * {@link #getPropertyDefinitions()} instead in that case.
     *
     * @return the non-null list of properties, empty if none
     */
    public List<PropertyWrapper> getPropertyWrappers() {
        return allProperties;
    }

    /**
     * Gets all foreign key properties.
     *
     * @return non-null list of PropertyWrappers, empty if no foreign key properties
     */
    public List<PropertyWrapper> getForeignKeyPropertyWrappers() {
        return foreignKeyProperties;
    }

    /**
     * Gets all foreign key properties as property definitions.
     *
     * @return a non-null list of PropertyWrapperDefinitions, empty if no foreign key properties
     */
    public List<PropertyWrapperDefinition> getForeignKeyPropertyWrapperDefinitions() {
        return classWrapper.getForeignKeyPropertyDefinitions();
    }

    /**
     * Gets all property definitions that are annotated with {@code DBColumn}.
     * This method is intended for where you need to examine meta-information
     * about all properties in a class.
     *
     * @return a list of PropertyWrapperDefinitions for the PropertyWrappers of this RowDefinition
     */
    public List<PropertyWrapperDefinition> getPropertyDefinitions() {
        return classWrapper.getPropertyDefinitions();
    }
}
