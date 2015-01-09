package nz.co.gregs.dbvolution.internal.properties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.exceptions.DBPebkacException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.internal.properties.JavaPropertyFinder.PropertyType;
import nz.co.gregs.dbvolution.internal.properties.JavaPropertyFinder.Visibility;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Wraps the class-type of an end-user's data model object. Generally it's
 * expected that the class is annotated with DBvolution annotations to mark the
 * table name and the fields or bean properties that map to columns, however
 * this class will work against any class type.
 *
 * <p>
 * To wrap a target object instance, use the
 * {@link #instanceWrapperFor(nz.co.gregs.dbvolution.query.RowDefinition) }
 * method.
 *
 * <p>
 * Note: instances of this class are expensive to create, and are intended to be
 * cached and kept long-term. Instances can be safely shared between DBDatabase
 * instances for different database types.
 *
 * <p>
 * Instances of this class are <i>thread-safe</i>.
 *
 * @author Malcolm Lett
 */
public class RowDefinitionClassWrapper {

	private final Class<? extends RowDefinition> adapteeClass;
	private final boolean identityOnly;
	private final TableHandler tableHandler;
	/**
	 * The property that forms the primary key, null if none.
	 */
	private final PropertyWrapperDefinition primaryKeyProperty;
	/**
	 * All properties of which DBvolution is aware, ordered as first
	 * encountered. Properties are only included if they are columns.
	 */
	private final List<PropertyWrapperDefinition> properties;
	/**
	 * Column names with original case for doing lookups on case-sensitive
	 * databases. If column names duplicated, stores only the first encountered
	 * of each column name. Assumes validation is done elsewhere in this class.
	 * Note: doesn't need to be synchronized because it's never modified once
	 * created.
	 */
	private final Map<String, PropertyWrapperDefinition> propertiesByCaseSensitiveColumnName;
	/**
	 * Column names normalized to upper case for doing lookups on
	 * case-insensitive databases. If column names duplicated, stores only the
	 * first encountered of each column name. Assumes validation is done
	 * elsewhere in this class. Note: doesn't need to be synchronized because
	 * it's never modified once created.
	 */
	private final Map<String, PropertyWrapperDefinition> propertiesByUpperCaseColumnName;
	/**
	 * Lists of properties that would have duplicated columns if-and-only-if
	 * using a case-insensitive database. For each duplicate upper case column
	 * name, lists all properties that have that same upper case column name.
	 *
	 * <p>
	 * We don't know in advance whether the database in use is case-insensitive
	 * or not. So we give case-different duplicates the benefit of doubt and
	 * just record until later. If this class is accessed for use on a
	 * case-insensitive database the exception will be thrown then, on first
	 * access to this class.
	 */
	private final Map<String, List<PropertyWrapperDefinition>> duplicatedPropertiesByUpperCaseColumnName;
	/**
	 * Indexed by java property name.
	 */
	private final Map<String, PropertyWrapperDefinition> propertiesByPropertyName;

	/**
	 * Fully constructs a wrapper for the given class, including performing all
	 * validations that can be performed up front.
	 *
	 * @param clazz the {@code DBRow} class to wrap
	 * @throws DBPebkacException on any validation errors
	 */
	public RowDefinitionClassWrapper(Class<? extends RowDefinition> clazz) {
		this(clazz, false);
	}

	/**
	 * Internal constructor only. Pass {@code processIdentityOnly=true} when
	 * processing a referenced class.
	 *
	 * <p>
	 * When processing identity only, only the primary key properties are
	 * identified.
	 *
	 
	 * @param processIdentityOnly pass {@code true} to only process the set of
	 * columns and primary keys, and to ensure that the primary key columns are
	 * valid, but to exclude all other validations on non-primary key columns
	 * and types etc.
	 */
	RowDefinitionClassWrapper(Class<? extends RowDefinition> clazz, boolean processIdentityOnly) {
		adapteeClass = clazz;
		identityOnly = processIdentityOnly;

		// annotation handlers
		tableHandler = new TableHandler(clazz);

        // pre-calculate properties list
		// (note: skip if processing identity only, in order to avoid
		//  all the per-property validation)
		properties = new ArrayList<PropertyWrapperDefinition>();
		if (processIdentityOnly) {
			// identity-only: extract only primary key properties
			JavaPropertyFinder propertyFinder = getJavaPropertyFinder();
			for (JavaProperty javaProperty : propertyFinder.getPropertiesOf(clazz)) {
				ColumnHandler column = new ColumnHandler(javaProperty);
				if (column.isColumn() && column.isPrimaryKey()) {
					PropertyWrapperDefinition property = new PropertyWrapperDefinition(this, javaProperty, processIdentityOnly);
					properties.add(property);
				}
			}
		} else {
			// extract all column properties
			int columnIndex = 0;	
			JavaPropertyFinder propertyFinder = getJavaPropertyFinder();
			for (JavaProperty javaProperty : propertyFinder.getPropertiesOf(clazz)) {
				PropertyWrapperDefinition property = new PropertyWrapperDefinition(this, javaProperty, processIdentityOnly);
				if (property.isColumn()) {
					columnIndex++;
					property.setColumnIndex(columnIndex);
					properties.add(property);
				}
			}
		}

		// pre-calculate primary key
		List<PropertyWrapperDefinition> primaryKeyProperties = new ArrayList<PropertyWrapperDefinition>();
		for (PropertyWrapperDefinition property : properties) {
			if (property.isPrimaryKey()) {
				primaryKeyProperties.add(property);
			}
		}
		if (primaryKeyProperties.size() > 1) {
			throw new UnsupportedOperationException("Multi-Column Primary Keys are not yet supported: Please remove the excess @PrimaryKey statements from " + clazz.getSimpleName());
		} else {
			this.primaryKeyProperty = primaryKeyProperties.isEmpty() ? null : primaryKeyProperties.get(0);
		}

		// pre-calculate properties index
		propertiesByCaseSensitiveColumnName = new HashMap<String, PropertyWrapperDefinition>();
		propertiesByUpperCaseColumnName = new HashMap<String, PropertyWrapperDefinition>();
		propertiesByPropertyName = new HashMap<String, PropertyWrapperDefinition>();
		duplicatedPropertiesByUpperCaseColumnName = new HashMap<String, List<PropertyWrapperDefinition>>();
		for (PropertyWrapperDefinition property : properties) {
			propertiesByPropertyName.put(property.javaName(), property);

            // add unique values for case-sensitive lookups
			// (error immediately on collisions)
			if (propertiesByCaseSensitiveColumnName.containsKey(property.getColumnName())) {
				if (!processIdentityOnly) {
					throw new DBPebkacException("Class " + clazz.getName() + " has multiple properties for column " + property.getColumnName());
				}
			} else {
				propertiesByCaseSensitiveColumnName.put(property.getColumnName(), property);
			}

            // add unique values for case-insensitive lookups
			// (defer erroring until actually know database is case insensitive)
			if (propertiesByUpperCaseColumnName.containsKey(property.getColumnName().toUpperCase())) {
				if (!processIdentityOnly) {
					List<PropertyWrapperDefinition> list = duplicatedPropertiesByUpperCaseColumnName.get(property.getColumnName().toUpperCase());
					if (list == null) {
						list = new ArrayList<PropertyWrapperDefinition>();
						list.add(propertiesByUpperCaseColumnName.get(property.getColumnName().toUpperCase()));
					}
					list.add(property);
					duplicatedPropertiesByUpperCaseColumnName.put(property.getColumnName().toUpperCase(), list);
				}
			} else {
				propertiesByUpperCaseColumnName.put(property.getColumnName().toUpperCase(), property);
			}
		}
	}

	/**
	 * Gets a new instance of the java property finder, configured as required
	 *
	 * @return A new JavePropertyFinder with the required settings
	 */
	private static JavaPropertyFinder getJavaPropertyFinder() {
		return new JavaPropertyFinder(
				Visibility.PRIVATE, Visibility.PUBLIC,
				JavaPropertyFilter.COLUMN_PROPERTY_FILTER,
				PropertyType.FIELD, PropertyType.BEAN_PROPERTY);
	}

	/**
	 * Checks for errors that can't be known in advance without knowing the
	 * database being accessed.
	 *
	 * @param database active database
	 */
	private void checkForRemainingErrorsOnAcccess(DBDatabase database) {
		// check for case-differing duplicate columns
		if (database.getDefinition().isColumnNamesCaseSensitive()) {
			if (!duplicatedPropertiesByUpperCaseColumnName.isEmpty()) {
				StringBuilder buf = new StringBuilder();
				for (List<PropertyWrapperDefinition> props : duplicatedPropertiesByUpperCaseColumnName.values()) {
					for (PropertyWrapperDefinition property : props) {
						if (buf.length() > 0) {
							buf.append(", ");
						}
						buf.append(property.getColumnName());
					}
				}

				throw new DBRuntimeException("The following columns are referenced multiple times on case-insensitive databases: " + buf.toString());
			}
		}
	}

	/**
	 * Gets an object wrapper instance for the given target object
	 *
	 * @param target the {@code DBRow} instance
	 * @return A RowDefinitionInstanceWrapper for the supplied target.
	 */
	public RowDefinitionInstanceWrapper instanceWrapperFor(RowDefinition target) {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only DBRow class wrapper");
		}
//		checkForRemainingErrorsOnAcccess(database);
		return new RowDefinitionInstanceWrapper(this, target);
	}

	/**
	 * Gets a string representation suitable for debugging.
	 * 
	 * @return a string representation of this object.
	 */
	@Override
	public String toString() {
		if (isTable()) {
			return getClass().getSimpleName() + "<" + tableName() + ":" + adapteeClass.getName() + ">";
		} else {
			return getClass().getSimpleName() + "<no-table:" + adapteeClass.getName() + ">";
		}
	}

	/**
	 * Two {@code RowDefinitionClassWrappers} are equal if they wrap the same classes.
	 * @param obj	 obj	
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
		if (!(obj instanceof RowDefinitionClassWrapper)) {
			return false;
		}
		RowDefinitionClassWrapper other = (RowDefinitionClassWrapper) obj;
		if (adapteeClass == null) {
			if (other.adapteeClass != null) {
				return false;
			}
		} else if (!adapteeClass.equals(other.adapteeClass)) {
			return false;
		}
		return true;
	}

	/**
	 * Calculates the hash-code based on the hash-code of the wrapped class.
	 * @return the hash-code
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((adapteeClass == null) ? 0 : adapteeClass.hashCode());
		return result;
	}
	
	/**
	 * Gets the underlying wrapped class.
	 *
	 * @return the DBRow or Object wrapped by this instance.
	 */
	public Class<? extends RowDefinition> adapteeClass() {
		return adapteeClass;
	}

	/**
	 * Gets the simple name of the class being wrapped by this adaptor.
	 * <p>
	 * Use {@link #tableName()} for the name of the table mapped to this class.
	 *
	 * <p>
	 * Equivalent to {@code this.adaptee().getSimpleName();}
	 *
	 * @return the SimpleName of the class being wrapped.
	 */
	public String javaName() {
		return adapteeClass.getSimpleName();
	}

	/**
	 * Gets the fully qualified name of the class being wrapped by this adaptor.
	 * <p>
	 * Use {@link #tableName()} for the name of the table mapped to this class.
	 *
	 * @return the fully qualified name of the class being wrapped.
	 */
	public String qualifiedJavaName() {
		return adapteeClass.getName();
	}

	/**
	 * Indicates whether this class maps to a database table.
	 *
	 * @return TRUE if this RowDefinitionClassWrapper represents a database
	 * table or view, otherwise FALSE.
	 */
	public boolean isTable() {
		return tableHandler.isTable();
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
	 * <p>
	 * Use {@link TableHandler#getDBTableNameAnnotation() } for low level
	 * access.
	 *
	 * @return the table name, if specified explicitly or implicitly.
	 */
	public String tableName() {
		return tableHandler.getTableName();
	}

	/**
	 * Gets the property that is the primary key, if one is marked. Note:
	 * multi-column primary key tables are not yet supported.
	 *
	 * @return the primary key property or null if no primary key
	 */
	public PropertyWrapperDefinition primaryKeyDefinition() {
		return primaryKeyProperty;
	}

	/**
	 * Gets the property associated with the given column. If multiple
	 * properties are annotated for the same column, this method will return
	 * only the first.
	 *
	 * <p>
	 * Only provides access to properties annotated with {@code DBColumn}.
	 *
	 * <p>
	 * Assumes validation is applied elsewhere to prohibit duplication of column
	 * names.
	 *
	 * @param database active database
	 * @param columnName columnName
	 columnName
	
	 * @return the PropertyWrapperDefinition for the column name supplied.
	 *         Null if no such column is found.
	 * @throws AssertionError if called when in {@code identityOnly} mode.
	 */
	public PropertyWrapperDefinition getPropertyDefinitionByColumn(DBDatabase database, String columnName) {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only DBRow class wrapper");
		}

		checkForRemainingErrorsOnAcccess(database);
		if (database.getDefinition().isColumnNamesCaseSensitive()) {
			return propertiesByUpperCaseColumnName.get(columnName.toUpperCase());
		} else {
			return propertiesByCaseSensitiveColumnName.get(columnName);
		}
	}

	/**
	 * Like {@link #getPropertyDefinitionByColumn(DBDatabase, String)} except
	 * that handles the case where the database definition is not yet known, and
	 * thus returns all possible matching properties by column name.
	 *
	 * <p>
	 * Assumes working in "identity-only" mode.
	 *
	 
	 * @return the non-null list of matching property definitions, with only
	 * identity information available, empty if no such properties found
	 */
	List<PropertyWrapperDefinition> getPropertyDefinitionIdentitiesByColumnNameCaseInsensitive(String columnName) {
		List<PropertyWrapperDefinition> list = new ArrayList<PropertyWrapperDefinition>();
		JavaPropertyFinder propertyFinder = getJavaPropertyFinder();
		for (JavaProperty javaProperty : propertyFinder.getPropertiesOf(adapteeClass)) {
			ColumnHandler column = new ColumnHandler(javaProperty);
			if (column.isColumn() && column.getColumnName().equalsIgnoreCase(columnName)) {
				PropertyWrapperDefinition property = new PropertyWrapperDefinition(this, javaProperty, true);
				list.add(property);
			}
		}
		return list;
	}

	/**
	 * Gets the property by its java property name.
	 * <p>
	 * Only provides access to properties annotated with {@code DBColumn}.
	 *
	 * <p>
	 * It's legal for a field and bean-property to have the same name, and to
	 * both be annotated, but for different columns. This method doesn't handle
	 * that well and returns only the first one it sees.
	 *
	 * @param propertyName	 propertyName	
	 * @return the PropertyWrapperDefinition for the named object property
	 *         Null if no such property is found.
	 * @throws AssertionError if called when in {@code identityOnly} mode.
	 */
	public PropertyWrapperDefinition getPropertyDefinitionByName(String propertyName) {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only DBRow class wrapper");
		}
		return propertiesByPropertyName.get(propertyName);
	}

	/**
	 * Gets all properties annotated with {@code DBColumn}.
	 *
	 * @return a List of all PropertyWrapperDefinitions for the wrapped class.
	 */
	public List<PropertyWrapperDefinition> getPropertyDefinitions() {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only DBRow class wrapper");
		}
		return properties;
	}

	/**
	 * Gets all foreign key properties.
	 *
	 * @return a list of ProperyWrapperDefinitions for all the foreign keys
	 * defined in the wrapped object
	 */
	public List<PropertyWrapperDefinition> getForeignKeyPropertyDefinitions() {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only DBRow class wrapper");
		}

		List<PropertyWrapperDefinition> list = new ArrayList<PropertyWrapperDefinition>();
		for (PropertyWrapperDefinition property : properties) {
			if (property.isColumn() && property.isForeignKey()) {
				list.add(property);
			}
		}
		return list;
	}
}
