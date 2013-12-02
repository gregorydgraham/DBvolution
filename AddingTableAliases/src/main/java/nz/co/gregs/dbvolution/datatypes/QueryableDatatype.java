/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.UnableInstantiateQueryableDatatypeException;
import nz.co.gregs.dbvolution.exceptions.UnableToCopyQueryableDatatypeException;
import nz.co.gregs.dbvolution.operators.*;

/**
 *
 * @author gregory.graham
 */
// FIXME: literalValue has a broken type: sometimes it's the original QDT's native type,
// other times it ends up as a string.
// eg: DBInteger myInt = new DBInteger(23) -> literalValue is an Integer (23).
// then do myInt.setValue(23) -> now literalValue is a String ("23").
// This will break the equals() tests in setChanged().
// It also breaks malcolm's new type adaptor logic.
public abstract class QueryableDatatype extends Object implements Serializable {

    public static final long serialVersionUID = 1L;
    protected Object literalValue = null;
    protected boolean isDBNull = false;
    protected boolean includingNulls = false;
    protected DBOperator operator = null;
    protected boolean undefined = true;
    protected boolean changed = false;
    protected QueryableDatatype previousValueAsQDT = null;
    protected boolean isPrimaryKey;
    public final static Boolean SORT_ASCENDING = Boolean.TRUE;
    public final static Boolean SORT_DESCENDING = Boolean.FALSE;
    protected Boolean sort = SORT_ASCENDING;

    QueryableDatatype() {
    }

    QueryableDatatype(String str) {
        if (str == null) {
            this.isDBNull = true;
        } else if (!str.isEmpty()) {
            this.literalValue = str;
            this.operator = new DBEqualsOperator(this);
        }
    }

    QueryableDatatype(Object str) {
        if (str == null) {
            this.isDBNull = true;
        } else if (!str.toString().isEmpty()) {
            this.literalValue = str;
            this.operator = new DBEqualsOperator(this);
        }
    }

    @Override
    public String toString() {
        return (literalValue == null ? "" : literalValue.toString());
    }

    /**
     * Copies a QueryableDatatype and returns the copy.
     *
     * Used internally to provide immutability to DBOperator objects.
     *
     * The intention is that this method will provide a snapshot of the QDT at
     * this moment in time and copy or clone any internal objects that might
     * change.
     *
     * Subclasses should extend this method if they have fields that maintain
     * the state of the QDT.
     *
     * Always use the super.copy() method first when overriding this method.
     *
     * @return
     */
    public QueryableDatatype copy() {
        QueryableDatatype newQDT = this;
        try {
            newQDT = this.getClass().newInstance();

            newQDT.literalValue = this.literalValue;
            newQDT.isDBNull = this.isDBNull;
            newQDT.includingNulls = this.includingNulls;
            newQDT.operator = this.operator;
            newQDT.undefined = this.undefined;
            newQDT.changed = this.changed;
            if (this.previousValueAsQDT != null) {
                newQDT.previousValueAsQDT = this.previousValueAsQDT.copy();
            }
            newQDT.isPrimaryKey = this.isPrimaryKey;
            newQDT.sort = this.sort;
        } catch (InstantiationException ex) {
            throw new UnableInstantiateQueryableDatatypeException(this, ex);
        } catch (IllegalAccessException ex) {
            throw new UnableToCopyQueryableDatatypeException(this, ex);
        }

        return newQDT;
    }

    /**
     * Returns the raw value as a String
     *
     * @return
     */
    public String stringValue() {
        return (literalValue == null ? null : literalValue.toString());
    }

    public Long longValue() {
        if (isDBNull || literalValue == null) {
            return null;
        } else if (literalValue instanceof Long) {
            return (Long) literalValue;
        } else if (literalValue instanceof Number) {
            return ((Number) literalValue).longValue();
        } else {
            return Long.parseLong(literalValue.toString());
        }

    }

    public Integer intValue() {
        if (isDBNull || literalValue == null) {
            return null;
        } else if (literalValue instanceof Integer) {
            return (Integer) literalValue;
        } else if (literalValue instanceof Number) {
            return ((Number) literalValue).intValue();
        } else {
            return Integer.parseInt(literalValue.toString());
        }

    }

    public Double doubleValue() {
        if (isDBNull || literalValue == null) {
            return null;
        } else if (literalValue instanceof Double) {
            return (Double) literalValue;
        } else if (literalValue instanceof Number) {
            return ((Number) literalValue).doubleValue();
        } else {
            return Double.parseDouble(literalValue.toString());
        }
    }

    protected void blankQuery() {
        includingNulls = false;
        isDBNull = false;
        this.operator = null;
    }

    public static <T extends QueryableDatatype> T getQueryableDatatypeInstance(Class<T> requiredQueryableDatatype) {
        try {
            return requiredQueryableDatatype.getConstructor().newInstance();
        } catch (NoSuchMethodException ex) {
            throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (SecurityException ex) {
            throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (InstantiationException ex) {
            throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        } catch (InvocationTargetException ex) {
            throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
        }
    }

    static QueryableDatatype getQueryableDatatypeForObject(Object o) {
        if (o instanceof Integer) {
            return new DBInteger();
        } else if (o instanceof Number) {
            return new DBNumber();
        } else if (o instanceof String) {
            return new DBString();
        } else if (o instanceof Date) {
            return new DBDate();
        } else if (o instanceof Byte[]) {
            return new DBByteArray();
        } else if (o instanceof Boolean) {
            return new DBBoolean();
        } else {
            return new DBJavaObject();
        }
    }

    /**
     *
     * @param columnName
     * @return
     */
    public String getWhereClause(DBDatabase db, String columnName) {
        return getWhereClauseUsingOperators(db, columnName);
    }

    private String getWhereClauseUsingOperators(DBDatabase db, String columnName) {
        String whereClause = "";
        DBOperator op = this.getOperator();
        if (op != null) {
            whereClause = op.generateWhereLine(db, columnName);
        }
        return whereClause;
    }

    public void negateOperator() {
        if (getOperator() != null) {
            getOperator().invertOperator(true);
        } else {
            throw new RuntimeException("No Operator Has Been Defined Yet: please use the query methods before inverting the operation");
        }
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public <T> void permittedValues(T... permitted) {
        if (permitted == null) {
            useNullOperator();
        } else if (permitted.length == 1) {
            if (permitted[0] instanceof List) {
                List<?> myList = (List) permitted[0];
                permittedValues(myList.toArray());
            } else if (permitted[0] instanceof Set) {
                Set<?> mySet = (Set) permitted[0];
                permittedValues(mySet.toArray());
            } else {
                useEqualsOperator(permitted[0]);
            }
        } else {
            useInOperator(permitted);
        }
    }

    /**
     *
     * @param permitted
     */
    public void permittedValuesIgnoreCase(String... permitted) {
        if (permitted == null) {
            useNullOperator();
        } else if (permitted.length == 1) {
            useEqualsCaseInsensitiveOperator(permitted[0]);
        } else {
            useInCaseInsensitiveOperator(permitted);
        }
    }

    /**
     *
     * @param permitted
     */
    public void permittedValuesIgnoreCase(List<String> permitted) {
        if (permitted == null) {
            useNullOperator();
        } else if (permitted.size() == 1) {
            useEqualsCaseInsensitiveOperator(permitted.get(0));
        } else {
            useInCaseInsensitiveOperator(permitted.toArray(new String[]{}));
        }
    }

    /**
     *
     * @param permitted
     */
    public void permittedValuesIgnoreCase(Set<String> permitted) {
        if (permitted == null) {
            useNullOperator();
        } else {
            useInCaseInsensitiveOperator(permitted.toArray(new String[]{}));
        }
    }

    /**
     *
     * @param permitted
     */
    public void excludedValuesIgnoreCase(String... permitted) {
        if (permitted == null) {
            useNullOperator();
        } else if (permitted.length == 1) {
            useEqualsCaseInsensitiveOperator(permitted[0]).not();
        } else {
            useInCaseInsensitiveOperator(permitted).not();
        }
    }

    /**
     *
     * @param permitted
     */
    public void excludedValuesIgnoreCase(List<String> permitted) {
        if (permitted == null) {
            useNullOperator();
        } else if (permitted.size() == 1) {
            useEqualsCaseInsensitiveOperator(permitted.get(0)).not();
        } else {
            useInCaseInsensitiveOperator(permitted.toArray(new String[]{})).not();
        }
    }

    /**
     *
     * @param excluded
     */
    public void excludedValuesIgnoreCase(Set<String> excluded) {
        if (excluded == null) {
            useNullOperator();
        } else {
            useInCaseInsensitiveOperator(excluded.toArray(new String[]{})).not();
        }
    }

    /**
     *
     * excludes the object, Set, List, Array, or vararg of objects
     *
     *
     * @param excluded
     */
    public <T> void excludedValues(T... excluded) {
        if (excluded == null) {
            useNullOperator().not();
        } else if (excluded.length == 1) {
            if (excluded[0] instanceof List) {
                List<?> myList = (List) excluded[0];
                excludedValues(myList.toArray());
            } else if (excluded[0] instanceof Set) {
                Set<?> mySet = (Set) excluded[0];
                excludedValues(mySet.toArray());
            } else {
                useEqualsOperator(excluded[0]).not();
            }
        } else {
            useInOperator(excluded).not();
        }
    }

    public void permittedRange(Object lowerBound, Object upperBound) {
        if (lowerBound != null && upperBound != null) {
            useBetweenOperator(lowerBound, upperBound);
        } else if (lowerBound == null && upperBound != null) {
            QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(upperBound);
            qdt.setValue(upperBound);
            useLessThanOperator(qdt);
        } else if (lowerBound != null && upperBound == null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(lowerBound);
            qdt.setValue(lowerBound);
            useGreaterThanOperator(qdt);
        }
    }

    public void permittedRangeInclusive(Object lowerBound, Object upperBound) {
        if (lowerBound != null && upperBound != null) {
            useBetweenOperator(lowerBound, upperBound);
        } else if (lowerBound == null && upperBound != null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(upperBound);
            qdt.setValue(upperBound);
            useLessThanOrEqualToOperator(qdt);
        } else if (lowerBound != null && upperBound == null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(lowerBound);
            qdt.setValue(lowerBound);
            useGreaterThanOrEqualToOperator(qdt);
        }
    }

    public void excludedRange(Object lowerBound, Object upperBound) {
        if (lowerBound != null && upperBound != null) {
            useBetweenOperator(lowerBound, upperBound).not();
        } else if (lowerBound == null && upperBound != null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(upperBound);
            qdt.setValue(upperBound);
            useLessThanOperator(qdt).not();
        } else if (lowerBound != null && upperBound == null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(lowerBound);
            qdt.setValue(lowerBound);
            useGreaterThanOperator(qdt).not();
        }
    }

    public void excludedRangeInclusive(Object lowerBound, Object upperBound) {
        if (lowerBound != null && upperBound != null) {
            useBetweenOperator(lowerBound, upperBound).not();
        } else if (lowerBound == null && upperBound != null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(upperBound);
            qdt.setValue(upperBound);
            useLessThanOrEqualToOperator(qdt).not();
        } else if (lowerBound != null && upperBound == null) {
            final QueryableDatatype qdt = QueryableDatatype.getQueryableDatatypeForObject(lowerBound);
            qdt.setValue(lowerBound);
            useGreaterThanOrEqualToOperator(qdt).not();
        }
    }

    public void permittedPattern(String pattern) {
        useLikeOperator(pattern);
    }

    public void excludedPattern(String pattern) {
        useLikeOperator(pattern).not();
    }

    /**
     *
     * @return
     */
    public Object getValue() {
        return literalValue;
    }

    /**
     *
     * @param newLiteralValue the literalValue to set
     */
    public void setValue(Object newLiteralValue) {
        useEqualsOperator(newLiteralValue);
    }

    protected DBOperator useEqualsOperator(Object newLiteralValue) {
        preventChangeOfPrimaryKey();
        blankQuery();
        if (newLiteralValue == null) {
            return useNullOperator();
        } else {
            if (newLiteralValue instanceof Date) {
                setChanged((Date) newLiteralValue);
                this.literalValue = newLiteralValue;
                this.setOperator(new DBEqualsOperator(new DBDate((Date) newLiteralValue)));
            } else if (newLiteralValue instanceof Timestamp) {
                setChanged((Timestamp) newLiteralValue);
                this.literalValue = newLiteralValue;
                this.setOperator(new DBEqualsOperator(new DBDate((Timestamp) newLiteralValue)));
            } else {
                setChanged(newLiteralValue);
                this.literalValue = newLiteralValue;
                this.setOperator(new DBEqualsOperator(this));
            }
        }
        return getOperator();
    }

    protected DBOperator useEqualsCaseInsensitiveOperator(String newLiteralValue) {
        preventChangeOfPrimaryKey();
        blankQuery();
        if (newLiteralValue == null) {
            return useNullOperator();
        } else {
            setChanged(newLiteralValue);
            this.literalValue = newLiteralValue.toString();
            this.setOperator(new DBEqualsCaseInsensitiveOperator(this));
        }
        return getOperator();
    }

    public void setUnchanged() {
        changed = false;
        previousValueAsQDT = null;
    }

    protected DBOperator useGreaterThanOperator(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            useNullOperator();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBGreaterThanOperator(literalValue));
        }
        return getOperator();
    }

    protected DBOperator useGreaterThanOrEqualToOperator(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            useNullOperator();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBGreaterThanOrEqualsOperator(literalValue));
        }
        return getOperator();
    }

    protected DBOperator useLessThanOperator(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            useNullOperator();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBLessThanOperator(literalValue));
        }
        return getOperator();
    }

    protected DBOperator useLessThanOrEqualToOperator(QueryableDatatype literalValue) {
        blankQuery();
        if (literalValue == null) {
            useNullOperator();
        } else {
            this.literalValue = literalValue.literalValue;
            this.setOperator(new DBLessThanOrEqualOperator(literalValue));
        }
        return getOperator();
    }

    /**
     *
     * Sets the value of this column to DBNull Also changes the operator to
     * DBIsNullOperator for comparisons
     *
     */
    protected DBOperator useNullOperator() {
        blankQuery();
        this.literalValue = null;
        this.isDBNull = true;
        this.setOperator(new DBIsNullOperator());
        return getOperator();
    }

    protected DBOperator useLikeOperator(Object t) {
        blankQuery();
        this.literalValue = t;
        this.setOperator(new DBLikeCaseInsensitiveOperator(this));
        return getOperator();
    }

    public void includingNulls() {
        this.operator.includeNulls();
    }

    /**
     * Converts the objects to QueryableDatatypes and calls
     * isIn(QueryableDatatype... inValues) with them
     *
     * @param inValues the inValues to set
     */
    protected <T> DBOperator useInOperator(T... inValues) {
        blankQuery();
        ArrayList<QueryableDatatype> inVals = new ArrayList<QueryableDatatype>();
        for (Object obj : inValues) {
            QueryableDatatype qdt = getQueryableDatatypeForObject(obj);
            qdt.setValue(obj);
            inVals.add(qdt);
        }
        this.setOperator(new DBInOperator(inVals));
        return getOperator();
    }

    protected DBOperator useInCaseInsensitiveOperator(String... inValues) {
        blankQuery();
        ArrayList<QueryableDatatype> inVals = new ArrayList<QueryableDatatype>();
        for (Object obj : inValues) {
            QueryableDatatype qdt = getQueryableDatatypeForObject(obj);
            qdt.setValue(obj);
            inVals.add(qdt);
        }
        this.setOperator(new DBInCaseInsensitiveOperator(inVals));
        return getOperator();
    }

    /**
     *
     * @param inValues
     */
    protected DBOperator useInOperator(QueryableDatatype... inValues) {
        blankQuery();
        ArrayList<QueryableDatatype> arrayList = new ArrayList<QueryableDatatype>();
        arrayList.addAll(Arrays.asList(inValues));
        this.setOperator(new DBInOperator(arrayList));
        return getOperator();
    }

    /**
     * @param lowerBound the lower bound to set
     * @param upperBound the upper bound to set
     */
    protected DBOperator useBetweenOperator(QueryableDatatype lowerBound, QueryableDatatype upperBound) {
        blankQuery();
        this.setOperator(new DBBetweenOperator(lowerBound, upperBound));
        return getOperator();
    }

    protected DBOperator useBetweenOperator(Object lowerBound, Object upperBound) {
        blankQuery();
        QueryableDatatype lower = getQueryableDatatypeForObject(lowerBound);
        lower.setValue(lowerBound);
        QueryableDatatype upper = getQueryableDatatypeForObject(upperBound);
        upper.setValue(upperBound);
        useBetweenOperator(lower, upper);
        return getOperator();
    }

    /**
     *
     * Provides the SQL datatype used by default for this type of object
     *
     * This should be overridden in each subclass
     *
     * Example return value: "VARCHAR(1000)"
     *
     * @return
     */
    public abstract String getSQLDatatype();

    /**
     *
     * @param db
     * @return the literal value as it would appear in an SQL statement i.e.
     * {yada} => 'yada' {} => NULL
     */
    public final String toSQLString(DBDatabase db) {
        DBDefinition def = db.getDefinition();
        if (this.isDBNull || literalValue == null) {
            return def.getNull();
        }
        return formatValueForSQLStatement(db);
    }

    /**
     *
     * Returns the value of the object formatted for the database
     *
     * This should be overridden in each subclass
     *
     * This method is called by toSQLString after checking for NULLs and should
     * return a string representation of the object formatted for use within a
     * SQL select, insert, update, or delete statement.
     *
     * For Example:
     *
     * DBString{yada} => 'yada'
     *
     * DBInteger{1234} => 123
     *
     * DBDate{1/March/2013} => TO_DATE('20130301', 'YYYYMMDD')
     *
     * @param db
     * @return
     */
    protected abstract String formatValueForSQLStatement(DBDatabase db);

    /**
     * @return the operator
     */
    public DBOperator getOperator() {
        return operator;
    }

    /**
     * @param operator the operator to set
     */
    public void setOperator(DBOperator operator) {
        this.operator = operator;
        if (undefined) {
            undefined = false;
        } else {
            changed = true;
        }
    }

    public boolean hasChanged() {
        return changed;
    }

    /**
     *
     * @param resultSet
     * @param resultSetColumnName
     * @throws SQLException
     */
    public void setFromResultSet(ResultSet resultSet, String resultSetColumnName) throws SQLException {
        if (resultSet == null || resultSetColumnName == null) {
            this.useNullOperator();
        } else {
            String dbValue;
            try {
                dbValue = resultSet.getString(resultSetColumnName);
                if (resultSet.wasNull()) {
                    dbValue = null;
                }
            } catch (SQLException ex) {
                // Probably means the column wasn't selected.
                dbValue = null;
            }
            if (dbValue == null) {
                this.useNullOperator();
            } else {
                this.useEqualsOperator(dbValue);
            }
        }
    }

    void setIsPrimaryKey(boolean b) {
        this.isPrimaryKey = b;
    }

    private void preventChangeOfPrimaryKey() {
        if (this.isPrimaryKey && !this.undefined) {
            throw new RuntimeException("Accidental Change Of Primary Key Stopped: Use the changePrimaryKey() method to change the primary key's value.");
        }
    }

    private void setChanged(Object newLiteralValue) {
        if (this.isDBNull
                || (literalValue != null && !newLiteralValue.equals(literalValue))) {
            changed = true;
            QueryableDatatype newInstance = QueryableDatatype.getQueryableDatatypeInstance(this.getClass());
            if (this.isDBNull) {
                newInstance.useNullOperator();
            } else {
                newInstance.useEqualsOperator(this.literalValue);
            }
            previousValueAsQDT = newInstance;
        }
    }

    public boolean isNull() {
        return isDBNull || literalValue == null;
    }

    public String getPreviousSQLValue(DBDatabase db) {
        return previousValueAsQDT.toSQLString(db);
    }

    /**
     * Used to switch the direction of the column's sort order
     *
     * Use Boolean.TRUE for Ascending Use Boolean.FALSE for Descending
     *
     * @param order
     * @return
     */
    private QueryableDatatype setSortOrder(Boolean order) {
        sort = order;
        return this;
    }

    /**
     * Used to switch the direction of the column's sort order
     *
     * @return this object
     */
    public QueryableDatatype setSortOrderAscending() {
        return this.setSortOrder(true);
    }

    /**
     * Used to switch the direction of the column's sort order
     *
     * @return this object
     */
    public QueryableDatatype setSortOrderDescending() {
        return this.setSortOrder(false);
    }

    public Boolean getSortOrder() {
        return sort;
    }

    public void clear() {
        blankQuery();
    }

    public boolean equals(QueryableDatatype other) {
        if (other == null) {
            return false;
        } else if (this.operator == null && other.operator == null) {
            return true;
        } else if (this.operator != null && other.operator == null) {
            return false;
        } else if (this.operator == null && other.operator != null) {
            return false;
        } else {
            return this.getOperator().equals(other.getOperator());
        }
    }
}
