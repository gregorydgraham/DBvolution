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
package nz.co.gregs.dbvolution;

import edu.uci.ics.jung.algorithms.layout.*;
import edu.uci.ics.jung.visualization.*;
import edu.uci.ics.jung.visualization.control.*;
import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;
import edu.uci.ics.jung.visualization.renderers.DefaultEdgeLabelRenderer;
import java.awt.Color;
import java.awt.Dimension;
import nz.co.gregs.dbvolution.query.DBRelationship;
import java.io.PrintStream;
import java.sql.*;
import java.util.*;
import javax.swing.JFrame;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;

import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DBDataComparison;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;
import nz.co.gregs.dbvolution.operators.DBOperator;
import nz.co.gregs.dbvolution.query.QueryGraph;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * The Definition of a Query on a Database
 *
 * <p>
 * DBvolution is available on <a
 * href="https://sourceforge.net/projects/dbvolution/">SourceForge</a> complete
 * with <a href="https://sourceforge.net/p/dbvolution/blog/">BLOG</a>
 *
 * <p>
 * DBQuery brings together several DBRow classes into a single database query.
 *
 * <p>
 * Natural joins are created while protecting against accidental Cartesian Joins
 * and Blank Queries.
 *
 * <p>
 * A DBQuery is most easily created by calling
 * {@link DBDatabase#getDBQuery(nz.co.gregs.dbvolution.DBRow...) DBDatabase's getDBQuery method}.
 *
 * <p>
 * The foreign keys from the DBRow instances will be automatically aligned and
 * the criteria defined on the DBRows will be seamlessly added to the WHERE
 * clause.
 *
 * <p>
 * Outer joins are supported using
 * {@link #addOptional(nz.co.gregs.dbvolution.DBRow...) addOptional}, as well as
 * "all OR" queries with {@link #setToMatchAnyCondition()} ( all or is a query
 * like SELECT .. FROM ... WHERE a=b OR b=c OR c=d ...)
 *
 * <p>
 * more complicated conditions can be added to the query itself using the
 * {@link #addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) addCondition method}.
 *
 * <p>
 * DBQuery can even scan the Class path and find all related DBRow classes and
 * add them on request.
 *
 * @author gregorygraham
 */
public class DBQuery {

	private static final int COUNT_QUERY = 1;
	private static final int SELECT_QUERY = 0;
	private final DBDatabase database;
	private final List<DBRow> queryTables;
	private final List<Class<? extends DBRow>> optionalQueryTables;
	private final List<DBRow> allQueryTables;
	private List<DBQueryRow> results;
	private String resultSQL;
	private final Map<Class<?>, Map<String, DBRow>> existingInstances = new HashMap<Class<?>, Map<String, DBRow>>();
	private final List<DBDataComparison> comparisons = new ArrayList<DBDataComparison>();
	private final List<BooleanExpression> expressions = new ArrayList<BooleanExpression>();
	private final Map<Object, DBExpression> expressionColumns = new LinkedHashMap<Object, DBExpression>();
	private final Map<Object, DBExpression> groupByColumns = new LinkedHashMap<Object, DBExpression>();
	private final QueryOptions options = new QueryOptions();
	private List<PropertyWrapper> sortOrder = null;
	private String rawSQLClause = "";
	private final List<DBRow> extraExamples = new ArrayList<DBRow>();
	private QueryGraph queryGraph;
	private ColumnProvider[] sortOrderColumns;

	private DBQuery(DBDatabase database) {
		this.queryTables = new ArrayList<DBRow>();
		this.optionalQueryTables = new ArrayList<Class<? extends DBRow>>();
		this.allQueryTables = new ArrayList<DBRow>();
		this.database = database;
		this.results = null;
		this.resultSQL = null;
	}

	static DBQuery getInstance(DBDatabase database, DBRow... examples) {
		DBQuery dbQuery = new DBQuery(database);
		for (DBRow example : examples) {
			dbQuery.add(example);
		}
		return dbQuery;
	}

	/**
	 *
	 * Add a table to the query.
	 *
	 * <p>
	 * This method adds the DBRow to the list of required (INNER) tables.
	 *
	 * <p>
	 * Criteria (permitted and excluded values) from this instance will be
	 * automatically included in the query and an instance of this DBRow class
	 * will be created for each DBQueryRow returned.
	 *
	 * @param tables a list of DBRow objects that defines required tables and
	 * criteria
	 * @return this DBQuery instance
	 */
	public DBQuery add(DBRow... tables) {
		for (DBRow table : tables) {
			queryTables.add(table);
			allQueryTables.add(table);
			results = null;
			resultSQL = null;
		}
		return this;
	}

	/**
	 *
	 * Add a List of tables to the query.
	 *
	 * <p>
	 * This method adds the DBRows to the list of required (INNER) tables.
	 *
	 * <p>
	 * Criteria (permitted and excluded values) from these instances will be
	 * automatically included in the query and an instance of this DBRow class
	 * will be created for each DBQueryRow returned.
	 *
	 * @param tables a list of DBRow objects that defines required tables and
	 * criteria
	 * @return this DBQuery instance
	 */
	public DBQuery add(List<DBRow> tables) {
		for (DBRow table : tables) {
			queryTables.add(table);
			allQueryTables.add(table);
			results = null;
			resultSQL = null;
		}
		return this;
	}

	/**
	 * Add an optional table to this query
	 *
	 * <p>
	 * This method adds an optional (OUTER) table to the query.
	 *
	 * <p>
	 * The query will return an instance of this DBRow for each row found,
	 * though it may be a null instance as there was no matching row in the
	 * database.
	 *
	 * <p>
	 * Criteria (permitted and excluded values) specified in the supplied
	 * instance will be added to the query.
	 *
	 * @param tables a list of DBRow objects that defines optional tables and
	 * criteria
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery addOptional(DBRow... tables) {
		for (DBRow table : tables) {
			optionalQueryTables.add(table.getClass());
			allQueryTables.add(table);
			results = null;
			resultSQL = null;
		}
		return this;
	}

	/**
	 * Remove tables from the query
	 *
	 * <p>
	 * This method removes previously added tables from the query.
	 *
	 * <p>
	 * Previous results and SQL are discarded, and the query is set ready to be
	 * re-run.
	 *
	 * @param tables a list of DBRow instances to remove from the query
	 * @return this DBQuery instance
	 */
	public DBQuery remove(DBRow... tables) {
		for (DBRow table : tables) {
			queryTables.remove(table);
			optionalQueryTables.remove(table.getClass());
			allQueryTables.remove(table);
			results = null;
			resultSQL = null;
		}
		return this;
	}

	/**
	 * Generates and returns the actual SQL to be used by this query.
	 *
	 * <p>
	 * Good for debugging and great for DBAs, this is how you find out what
	 * DBvolution is really doing.
	 *
	 * <p>
	 * Generates the SQL query for retrieving the objects but does not execute
	 * the SQL. Use {@link #getAllRows() the get*Rows methods} to retrieve the
	 * rows.
	 *
	 * <p>
	 * See also {@link DBQuery#getSQLForCount() getSQLForCount}
	 *
	 * @return a String of the SQL that will be used by this DBQuery.
	 * @throws SQLException
	 */
	public String getSQLForQuery() throws SQLException {
		return getSQLForQuery(SELECT_QUERY);
	}

	String getANSIJoinClause(DBRow newTable, List<DBRow> previousTables) {
		List<String> joinClauses = new ArrayList<String>();
		String lineSep = System.getProperty("line.separator");
		DBDefinition defn = database.getDefinition();
		boolean isLeftOuterJoin = false;
		boolean isFullOuterJoin = false;
		if (queryTables.isEmpty() && optionalQueryTables.size() == allQueryTables.size()) {
			isFullOuterJoin = true;
		} else if (optionalQueryTables.contains(newTable.getClass())) {
			isLeftOuterJoin = true;
		}
		for (DBRow otherTable : previousTables) {
			String join = otherTable.getRelationshipsAsSQL(this.database, newTable, options);
			if (join != null && !join.isEmpty()) {
				joinClauses.add(join);
			}
		}
		String sqlToReturn;
		if (previousTables.isEmpty()) {
			sqlToReturn = " " + newTable.getTableName() + defn.beginTableAlias() + defn.getTableAlias(newTable) + defn.endTableAlias();
		} else {
			if (isFullOuterJoin) {
				sqlToReturn = lineSep + defn.beginFullOuterJoin();
			} else if (isLeftOuterJoin) {
				sqlToReturn = lineSep + defn.beginLeftOuterJoin();
			} else {
				sqlToReturn = lineSep + defn.beginInnerJoin();
			}
			sqlToReturn += newTable.getTableName() + defn.beginTableAlias() + defn.getTableAlias(newTable) + defn.endTableAlias();
			if (joinClauses.isEmpty()) {
				sqlToReturn += defn.beginOnClause() + defn.getWhereClauseBeginningCondition(options) + defn.endOnClause();
			} else {
				sqlToReturn += defn.beginOnClause();
				String separator = "";
				for (String join : joinClauses) {
					sqlToReturn += separator + join;
					separator = defn.beginWhereClauseLine(options);
				}
				sqlToReturn += defn.endOnClause();
			}
		}
		return sqlToReturn;
	}

	private String getSQLForQuery(int queryType) throws SQLException {

		if (allQueryTables.isEmpty()) {
			throw new AccidentalBlankQueryException();
		}

		if (!options.isBlankQueryAllowed() && willCreateBlankQuery() && rawSQLClause.isEmpty()) {
			throw new AccidentalBlankQueryException();
		}

		initialiseQueryGraph();

		if (!options.isCartesianJoinAllowed() && allQueryTables.size() > 1 && queryGraph.willCreateCartesianJoin()) {
			throw new AccidentalCartesianJoinException();
		}
		DBDefinition defn = database.getDefinition();
		StringBuilder selectClause = new StringBuilder().append(defn.beginSelectStatement());
		int columnIndex = 1;
		String groupByColumnIndex = defn.beginGroupByClause();
		String groupByColumnIndexSeparator = "";
		HashMap<PropertyWrapperDefinition, Integer> indexesOfSelectedColumns = new HashMap<PropertyWrapperDefinition, Integer>();
		HashMap<DBExpression, Integer> indexesOfSelectedExpressions = new HashMap<DBExpression, Integer>();
		StringBuilder fromClause = new StringBuilder().append(defn.beginFromClause());
		List<DBRow> joinedTables = new ArrayList<DBRow>();
		StringBuilder whereClause = new StringBuilder().append(defn.beginWhereClause()).append(defn.getWhereClauseBeginningCondition(options));
		StringBuilder groupByClause = new StringBuilder().append(defn.beginGroupByClause());
		String lineSep = System.getProperty("line.separator");
		DBRow startQueryFromTable = queryTables.isEmpty() ? allQueryTables.get(0) : queryTables.get(0);
		List<DBRow> sortedQueryTables = options.isCartesianJoinAllowed()
			? queryGraph.toListIncludingCartesian(startQueryFromTable.getClass())
			: queryGraph.toList(startQueryFromTable.getClass());

		if (options.getRowLimit() != null) {
			selectClause.append(defn.getLimitRowsSubClauseDuringSelectClause(options.getRowLimit()));
		}

		String separator = "";
		String colSep = defn.getStartingSelectSubClauseSeparator();
		String groupByColSep = "";
		String tableName;

		for (DBRow tabRow : sortedQueryTables) {
			tableName = tabRow.getTableName();

			List<PropertyWrapper> tabProps = tabRow.getSelectedProperties();
			for (PropertyWrapper propWrapper : tabProps) {
				selectClause.append(colSep).append(propWrapper.getSelectableName(database)).append(" ").append(propWrapper.getColumnAlias(database));
				colSep = defn.getSubsequentSelectSubClauseSeparator() + lineSep;

				// Now deal with the GROUP BY and ORDER BY clause requirements
				groupByColumnIndex += groupByColumnIndexSeparator + columnIndex;
				groupByColumnIndexSeparator = defn.getSubsequentGroupBySubClauseSeparator();
				indexesOfSelectedColumns.put(propWrapper.getDefinition(), columnIndex);
				columnIndex++;
			}
			if (!options.isUseANSISyntax()) {
				fromClause.append(separator).append(tableName);
			} else {
				fromClause.append(getANSIJoinClause(tabRow, joinedTables));
			}
			joinedTables.add(tabRow);
			List<String> tabRowCriteria = tabRow.getWhereClausesWithAliases(database);
			if (tabRowCriteria != null && !tabRowCriteria.isEmpty()) {
				for (String clause : tabRowCriteria) {
					whereClause.append(lineSep).append(defn.beginWhereClauseLine(options)).append(clause);
				}
			}

			if (!options.isUseANSISyntax()) {
				getNonANSIJoin(tabRow, whereClause, defn, joinedTables, lineSep);
			}

			separator = ", " + lineSep;
		}

		for (DBDataComparison comp : comparisons) {
			whereClause.append(lineSep).append(defn.beginWhereClauseLine(options)).append("(").append(comp.getOperator().generateWhereLine(database, comp.getLeftHandSide().toSQLString(database))).append(")");
		}

		for (BooleanExpression expression : expressions) {
			whereClause.append(lineSep).append(defn.beginWhereClauseLine(options)).append("(").append(expression.toSQLString(database)).append(")");
		}

		for (Map.Entry<Object, DBExpression> entry : expressionColumns.entrySet()) {
			final Object key = entry.getKey();
			final DBExpression expression = entry.getValue();
			selectClause.append(colSep).append(expression.toSQLString(database)).append(" ").append(defn.formatExpressionAlias(key));
			colSep = defn.getSubsequentSelectSubClauseSeparator() + lineSep;
			if (!expression.isAggregator()) {
				groupByColumnIndex += groupByColumnIndexSeparator + columnIndex;
				groupByColumnIndexSeparator = defn.getSubsequentGroupBySubClauseSeparator();
			}
			indexesOfSelectedExpressions.put(expression, columnIndex);
			columnIndex++;
		}

		for (Map.Entry<Object, DBExpression> entry : groupByColumns.entrySet()) {
			groupByClause.append(groupByColSep).append(entry.getValue().toSQLString(database));
			groupByColSep = defn.getSubsequentGroupBySubClauseSeparator() + lineSep;
		}

		for (DBRow extra : extraExamples) {
			List<String> extraCriteria = extra.getWhereClausesWithAliases(database);
			if (extraCriteria != null && !extraCriteria.isEmpty()) {
				for (String clause : extraCriteria) {
					whereClause.append(lineSep).append(defn.beginWhereClauseLine(options)).append(clause);
				}
			}
		}

		boolean useColumnIndexGroupBy = defn.prefersIndexBasedGroupByClause();

		String sqlString = "";
		if (queryType == SELECT_QUERY) {
			// Clean up the formatting of the optional clauses
			String rawSQLClauseFinal = (rawSQLClause.isEmpty() ? "" : rawSQLClause + lineSep);
			String groupByClauseFinal = (groupByColumns.size() > 0 ? (useColumnIndexGroupBy ? groupByColumnIndex : groupByClause.toString()) + lineSep : "");
			String orderByClauseFinal = getOrderByClause(indexesOfSelectedColumns, indexesOfSelectedExpressions);
			if (!orderByClauseFinal.trim().isEmpty()) {
				orderByClauseFinal += lineSep;
			}
			sqlString = selectClause.append(lineSep)
				.append(fromClause).append(lineSep)
				.append(whereClause).append(lineSep)
				.append(rawSQLClauseFinal)
				.append(groupByClauseFinal)
				.append(orderByClauseFinal)
				.append(options.getRowLimit() != null ? defn.getLimitRowsSubClauseAfterWhereClause(options.getRowLimit()) : "")
				.append(defn.endSQLStatement())
				.toString();
		} else if (queryType == COUNT_QUERY) {
			sqlString = defn.beginSelectStatement() + defn.countStarClause() + lineSep + fromClause + lineSep + whereClause + lineSep + rawSQLClause + lineSep + defn.endSQLStatement();
		}

		return sqlString;
	}

	private void getNonANSIJoin(DBRow tabRow, StringBuilder whereClause, DBDefinition defn, List<DBRow> otherTables, String lineSep) {
		for (DBRelationship rel : tabRow.getAdHocRelationships()) {
			whereClause.append(defn.beginWhereClauseLine(options)).append("(").append(rel.toSQLString(database)).append(")");
		}

		for (DBRow otherTab : otherTables) {
			List<PropertyWrapper> otherTableFks = otherTab.getForeignKeyPropertyWrappers();
			for (PropertyWrapper otherTableFk : otherTableFks) {
				Class<? extends DBRow> fkReferencedClass = otherTableFk.referencedClass();

				if (fkReferencedClass.isAssignableFrom(tabRow.getClass())) {
					String formattedForeignKey = defn.formatTableAliasAndColumnName(
						otherTab, otherTableFk.columnName());

					String formattedReferencedColumn = defn.formatTableAliasAndColumnName(
						tabRow, otherTableFk.referencedColumnName());

					whereClause
						.append(lineSep)
						.append(defn.beginWhereClauseLine(options))
						.append("(")
						.append(formattedForeignKey)
						.append(defn.getEqualsComparator())
						.append(formattedReferencedColumn)
						.append(")");
				}
			}
		}
	}

	/**
	 * Returns the SQL query that will used to count the rows
	 *
	 * <p>
	 * Use this method to check the SQL that will be executed during
	 * {@link DBQuery#count() the count() method}
	 *
	 * @return a String of the SQL query that will be used to count the rows
	 * returned by this query
	 * @throws SQLException
	 */
	public String getSQLForCount() throws SQLException {
		return getSQLForQuery(DBQuery.COUNT_QUERY);
	}

	/**
	 * Constructs the SQL for this DBQuery using the supplied DBRows as examples
	 * and executes it on the database, returning the rows found.
	 *
	 * <p>
	 * Adds all required DBRows as inner join tables and all optional DBRows as
	 * outer join tables. All criteria specified on the DBRows will be applied.
	 * <p>
	 * Uses the defined
	 * {@link nz.co.gregs.dbvolution.annotations.DBForeignKey foreign keys} on
	 * the DBRow and
	 * {@link nz.co.gregs.dbvolution.DBRow#addRelationship(nz.co.gregs.dbvolution.datatypes.QueryableDatatype, nz.co.gregs.dbvolution.DBRow, nz.co.gregs.dbvolution.datatypes.QueryableDatatype) added relationships}
	 * to connect the tables. Foreign keys that have been
	 * {@link nz.co.gregs.dbvolution.DBRow#ignoreForeignKey(java.lang.Object) ignored}
	 * are not used.
	 * <p>
	 * Criteria such as
	 * {@link DBNumber#permittedValues(java.lang.Number...)  permitted values}
	 * defined on the fields of the DBRow examples are added as part of the
	 * WHERE clause.
	 *
	 * <p>
	 * Similarly conditions added to the DBQuery using
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) addCondition}
	 * are added.
	 *
	 * @return A List of DBQueryRows containing all the DBRow instances aligned
	 * with their related instances.
	 * @throws SQLException
	 * @see DBRow
	 * @see DBForeignKey
	 * @see QueryableDatatype
	 * @see BooleanExpression
	 * @see DBDatabase
	 */
	public List<DBQueryRow> getAllRows() throws SQLException {
		results = new ArrayList<DBQueryRow>();
		resultSQL = this.getSQLForQuery();
		DBQueryRow queryRow;

		DBStatement dbStatement = database.getDBStatement();
		try {
			ResultSet resultSet = dbStatement.executeQuery(resultSQL);
			try {
				while (resultSet.next()) {
					queryRow = new DBQueryRow();

					for (Map.Entry<Object, DBExpression> entry : expressionColumns.entrySet()) {
						String expressionAlias = database.getDefinition().formatExpressionAlias(entry.getKey());
						QueryableDatatype expressionQDT = entry.getValue().getQueryableDatatypeForExpressionValue();
						expressionQDT.setFromResultSet(resultSet, expressionAlias);
						queryRow.addExpressionColumnValue(entry.getKey(), expressionQDT);
					}
					for (DBRow tableRow : allQueryTables) {
						DBRow newInstance = DBRow.getDBRow(tableRow.getClass());

						List<PropertyWrapper> newProperties = newInstance.getPropertyWrappers();
						for (PropertyWrapper newProp : newProperties) {
							QueryableDatatype qdt = newProp.getQueryableDatatype();

							String resultSetColumnName = newProp.getColumnAlias(database);
							qdt.setFromResultSet(resultSet, resultSetColumnName);
							if (newInstance.isEmptyRow() && !qdt.isNull()) {
								newInstance.setEmptyRow(false);
							}

							// ensure field set when using type adaptors
							newProp.setQueryableDatatype(qdt);
						}
						newInstance.setDefined(); // Actually came from the database so it is a defined row.
						Map<String, DBRow> existingInstancesOfThisTableRow = existingInstances.get(tableRow.getClass());
						if (existingInstancesOfThisTableRow == null) {
							existingInstancesOfThisTableRow = new HashMap<String, DBRow>();
							existingInstances.put(newInstance.getClass(), existingInstancesOfThisTableRow);
						}
						DBRow existingInstance = newInstance;
						final PropertyWrapper primaryKey = newInstance.getPrimaryKeyPropertyWrapper();
						if (primaryKey != null) {
							final QueryableDatatype qdt = primaryKey.getQueryableDatatype();
							if (qdt != null) {
								existingInstance = existingInstancesOfThisTableRow.get(qdt.toSQLString(this.database));
								if (existingInstance == null) {
									existingInstance = newInstance;
									existingInstancesOfThisTableRow.put(qdt.toSQLString(this.database), existingInstance);
								}
							}
						}
						queryRow.put(existingInstance.getClass(), existingInstance);
					}
					results.add(queryRow);
				}
			} finally {
				resultSet.close();
			}
		} finally {
			dbStatement.close();
		}
		return results;
	}

	/**
	 * Returns all the known instances of the exemplar.
	 *
	 * <p>
	 * Similar to
	 * {@link #getAllInstancesOf(nz.co.gregs.dbvolution.DBRow) getAllInstancesOf(DBRow)}
	 *
	 * <p>
	 * Expects there to be exactly one(1) object of the exemplar type.
	 *
	 * <p>
	 * An UnexpectedNumberOfRowsException is thrown if there is zero or more
	 * than one row.
	 *
	 * @param <R> a subclass of DBRow
	 * @param exemplar an instance of R
	 * @return the ONLY instance found using this query
	 * @throws SQLException
	 * @throws UnexpectedNumberOfRowsException
	 */
	public <R extends DBRow> R getOnlyInstanceOf(R exemplar) throws SQLException, UnexpectedNumberOfRowsException {
		List<R> allInstancesFound = getAllInstancesOf(exemplar, 1);
		return allInstancesFound.get(0);
	}

	/**
	 * Returns all the known instances of the exemplar.
	 *
	 * <p>
	 * A simple means of ensuring that your query has retrieved the correct
	 * results. For instance if you are looking up 2 vehicles in the database
	 * and 3 are returned, this method will throw an exception stopping the
	 * DBScript or DBTransaction automatically.
	 *
	 * <p>
	 * Similar to
	 * {@link #getAllInstancesOf(nz.co.gregs.dbvolution.DBRow) getAllInstancesOf(DBRow)}
	 *
	 * <p>
	 * Expects there to be exactly as many objects of the exemplar type as
	 * specified
	 *
	 * <p>
	 * An UnexpectedNumberOfRowsException is thrown if there is less or more
	 * instances than than specified.
	 *
	 *
	 * @param <R> a class that extends DBRow
	 * @param exemplar The DBRow class that you would like returned.
	 * @param expected The expected number of rows, an exception will be thrown
	 * if this expectation is not met.
	 * @return a list of all the instances of the exemplar found by this query
	 * @throws SQLException
	 * @throws UnexpectedNumberOfRowsException
	 */
	public <R extends DBRow> List<R> getAllInstancesOf(R exemplar, long expected) throws SQLException, UnexpectedNumberOfRowsException {
		List<R> allInstancesFound = getAllInstancesOf(exemplar);
		final int actual = allInstancesFound.size();
		if (actual > expected) {
			throw new UnexpectedNumberOfRowsException(expected, actual, "Too Many Results: expected " + expected + ", actually got " + actual);
		} else if (actual < expected) {
			throw new UnexpectedNumberOfRowsException(expected, actual, "Too Few Results: expected " + expected + ", actually got " + actual);
		} else {
			return allInstancesFound;
		}
	}

	private boolean needsResults() {
		try {
			return results == null
				|| results.isEmpty()
				|| resultSQL == null
				|| !resultSQL.equals(getSQLForQuery());
		} catch (SQLException ex) {
			return true;
		}
	}

	/**
	 * Finds all instances of the exemplar in the results and returns them.
	 *
	 * <p>
	 * Allows easy retrieval of all the examples of a DBRow class regardless of
	 * DBQueryRows they are in.
	 *
	 * <p>
	 * Facilitates processing of rows on a single table retrieved via a
	 * complicated series of tables.
	 *
	 * @param <R> a class that extends DBRow
	 * @param exemplar an instance of R that has been included in the query
	 * @return A List of all the instances found of the exemplar.
	 * @throws SQLException
	 */
	public <R extends DBRow> List<R> getAllInstancesOf(R exemplar) throws SQLException {
		List<R> arrayList = new ArrayList<R>();
		if (this.needsResults()) {
			getAllRows();
		}
		if (!results.isEmpty()) {
			for (DBQueryRow row : results) {
				final R found = row.get(exemplar);
				if (found != null) { // in case there are no items of the exemplar
					if (!arrayList.contains(found)) {
						arrayList.add(found);
					}
				}
			}
		}
		return arrayList;
	}

	/**
	 * Convenience method to print all the rows in the current collection
	 * Equivalent to: printAll(System.out);
	 *
	 * @throws java.sql.SQLException
	 */
	public void print() throws SQLException {
		print(System.out);
	}

	/**
	 * Fast way to print the results
	 *
	 * myTable.printRows(System.err);
	 *
	 * @param ps a printstream to print to.
	 * @throws java.sql.SQLException
	 */
	public void print(PrintStream ps) throws SQLException {
		if (needsResults()) {
			this.getAllRows();
		}

		for (DBQueryRow row : this.results) {
			for (DBRow tab : this.allQueryTables) {
				DBRow rowPart = row.get(tab);
				if (rowPart != null) {
					String rowPartStr = rowPart.toString();
					ps.print(rowPartStr);
				}
			}
			ps.println();
		}
	}

	/**
	 * Fast way to print the results.
	 *
	 * <p>
	 * Retrieves the rows if required and then prints all of the rows but only
	 * the fields that have non-null values.
	 *
	 * <p>
	 * Helps to trim a wide printout of columns down to only the data specified
	 * in the rows.
	 *
	 * <p>
	 * Example: myQuery.printAllDataColumns(System.err);
	 *
	 * @param printStream a printstream to print to
	 * @throws java.sql.SQLException
	 */
	public void printAllDataColumns(PrintStream printStream) throws SQLException {
		if (needsResults()) {
			this.getAllRows();
		}

		for (DBQueryRow row : this.results) {
			for (DBRow tab : this.allQueryTables) {
				DBRow rowPart = row.get(tab);
				if (rowPart != null) {
					String rowPartStr = rowPart.toString();
					printStream.print(rowPartStr);
				}
			}

			printStream.println();
		}
	}

	/**
	 * Fast way to print the results.
	 *
	 * <p>
	 * Retrieves and prints all the rows but only prints the primary key
	 * columns.
	 *
	 * <p>
	 * Example: myQuery.printAllPrimaryKeys(System.err);
	 *
	 * @param ps a printstream to print to.
	 * @throws java.sql.SQLException
	 */
	public void printAllPrimaryKeys(PrintStream ps) throws SQLException {
		if (needsResults()) {
			this.getAllRows();
		}

		for (DBQueryRow row : this.results) {
			for (DBRow tab : this.allQueryTables) {
				DBRow rowPart = row.get(tab);
				if (rowPart != null) {
					final QueryableDatatype primaryKey = rowPart.getPrimaryKey();
					if (primaryKey != null) {
						String rowPartStr = primaryKey.toSQLString(this.database);
						ps.print(" " + rowPart.getPrimaryKeyColumnName() + ": " + rowPartStr);
					}
				}
			}
			ps.println();
		}
	}

	/**
	 * Remove all tables from the query and discard any results or state.
	 *
	 * <p>
	 * Clears all the settings and collections within this instance and set it
	 * back to a blank state
	 *
	 * @return this DBQuery instance.
	 */
	public DBQuery clear() {
		this.queryTables.clear();
		this.optionalQueryTables.clear();
		this.allQueryTables.clear();
		this.comparisons.clear();
		this.expressions.clear();
		this.extraExamples.clear();
		results = null;
		return this;
	}

	/**
	 * Count the rows on the database without retrieving the rows.
	 *
	 * <p>
	 * Either: counts the results already retrieved, or creates a
	 * {@link #getSQLForCount() count query} for this instance and retrieves the
	 * number of rows that would have been returned had
	 * {@link #getAllRows() getAllRows()} been called.
	 *
	 * @return the number of rows that have or will be retrieved.
	 * @throws SQLException
	 */
	public Long count() throws SQLException {
		if (results != null) {
			return (long) results.size();
		} else {
			Long result = 0L;

			DBStatement dbStatement = database.getDBStatement();
			try {
				final String sqlForCount = this.getSQLForCount();
				ResultSet resultSet = dbStatement.executeQuery(sqlForCount);
				try {
					while (resultSet.next()) {
						result = resultSet.getLong(1);
					}
				} finally {
					resultSet.close();
				}
			} finally {
				dbStatement.close();
			}
			return result;
		}
	}

	/**
	 * Test whether this DBQuery will create a query without limitations.
	 *
	 * <p>
	 * Checks this instance for criteria and conditions and returns FALSE if at
	 * least one constraint has been placed on the query.
	 *
	 * <p>
	 * This helps avoid the common mistake of accidentally retrieving all the
	 * rows of the tables by forgetting to add criteria.
	 *
	 * <p>
	 * No attempt to compare the length of the query results with the length of
	 * the table is made: if your criteria selects all the row of the tables
	 * this method will still return FALSE.
	 *
	 * @return TRUE if the DBQuery will retrieve all the rows of the tables,
	 * FALSE otherwise
	 */
	public boolean willCreateBlankQuery() {
		boolean willCreateBlankQuery = true;
		for (DBRow table : allQueryTables) {
			willCreateBlankQuery = willCreateBlankQuery && table.willCreateBlankQuery(this.database);
		}
		for (DBRow table : extraExamples) {
			willCreateBlankQuery = willCreateBlankQuery && table.willCreateBlankQuery(this.database);
		}
		return willCreateBlankQuery && (comparisons.isEmpty()) && (expressions.isEmpty());
	}

	/**
	 * Limit the query to only returning a certain number of rows
	 *
	 * <p>
	 * Implements support of the LIMIT and TOP operators of many databases.
	 *
	 * <p>
	 * Only the specified number of rows will be returned from the database and
	 * DBvolution.
	 *
	 * @param maximumNumberOfRowsReturned the require limit to the number of
	 * rows returned
	 * @return this DBQuery instance
	 */
	public DBQuery setRowLimit(long maximumNumberOfRowsReturned) {
		options.setRowLimit(maximumNumberOfRowsReturned);
		results = null;

		return this;
	}

	/**
	 * Clear the row limit on this DBQuery and return it to retrieving all rows.
	 *
	 * <p>
	 * Also resets the retrieved results so that the database will be
	 * re-queried.
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery clearRowLimit() {
		options.setRowLimit(null);
		results = null;

		return this;
	}

	/**
	 * Sets the sort order of properties (field and/or method) by the given
	 * property object references.
	 *
	 * <p>
	 * For example the following code snippet will sort by just the name column:
	 * <pre>
	 * Customer customer = ...;
	 * query.setSortOrder(customer.column(customer.name));
	 * </pre>
	 *
	 * @param sortColumns a list of columns to sort the query by.
	 * @return this DBQuery instance
	 */
	public DBQuery setSortOrder(ColumnProvider... sortColumns) {
		results = null;

		sortOrderColumns = sortColumns;

		sortOrder = new ArrayList<PropertyWrapper>();
		PropertyWrapper prop;
		for (ColumnProvider col : sortColumns) {
			prop = col.getColumn().getPropertyWrapper();
			if (prop != null) {
				sortOrder.add(prop);
			}
		}

		return this;
	}

	/**
	 * Remove all sorting that has been set on this DBQuery
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery clearSortOrder() {
		sortOrder = null;
		sortOrderColumns = null;
		return this;
	}

	private String getOrderByClause(Map<PropertyWrapperDefinition, Integer> indexesOfSelectedProperties, Map<DBExpression, Integer> IndexesOfSelectedExpressions) {
		DBDefinition defn = database.getDefinition();
		final boolean prefersIndexBasedOrderByClause = defn.prefersIndexBasedOrderByClause();
		if (sortOrderColumns != null && sortOrderColumns.length > 0) {
			StringBuilder orderByClause = new StringBuilder(defn.beginOrderByClause());
			String sortSeparator = defn.getStartingOrderByClauseSeparator();
			for (ColumnProvider column : sortOrderColumns) {
				PropertyWrapper prop = column.getColumn().getPropertyWrapper();
				QueryableDatatype qdt = prop.getQueryableDatatype();
				PropertyWrapperDefinition propDefn = prop.getDefinition();
				if (prefersIndexBasedOrderByClause) {
					Integer columnIndex = indexesOfSelectedProperties.get(propDefn);
					if (columnIndex == null) {
						columnIndex = IndexesOfSelectedExpressions.get(qdt);
					}
					if (columnIndex == null) {
						final DBExpression columnExpression = qdt.getColumnExpression();
						columnIndex = IndexesOfSelectedExpressions.get(columnExpression);
					}
					orderByClause.append(sortSeparator).append(columnIndex).append(defn.getOrderByDirectionClause(qdt.getSortOrder()));
					sortSeparator = defn.getSubsequentOrderByClauseSeparator();
				} else {
					if (qdt.hasColumnExpression()) {
						final String dbColumnName = qdt.getColumnExpression().toSQLString(database);
						if (dbColumnName != null) {
							orderByClause.append(sortSeparator).append(dbColumnName).append(defn.getOrderByDirectionClause(qdt.getSortOrder()));
							sortSeparator = defn.getSubsequentOrderByClauseSeparator();
						}
					} else {
						final RowDefinition possibleDBRow = prop.getRowDefinitionInstanceWrapper().adapteeRowDefinition();

						if (possibleDBRow != null && DBRow.class.isAssignableFrom(possibleDBRow.getClass())) {
							final DBRow adapteeDBRow = (DBRow) possibleDBRow;
							final String dbColumnName = defn.formatTableAliasAndColumnName(adapteeDBRow, prop.columnName());
							if (dbColumnName
								!= null) {
								orderByClause.append(sortSeparator).append(dbColumnName).append(defn.getOrderByDirectionClause(qdt.getSortOrder()));
								sortSeparator = defn.getSubsequentOrderByClauseSeparator();
							}
						}
					}
				}
			}
			orderByClause.append(defn.endOrderByClause());
			return orderByClause.toString();
		}
		return "";
	}

	/**
	 * Change the Default Setting of Disallowing Blank Queries
	 *
	 * <p>
	 * A common mistake is creating a query without supplying criteria and
	 * accidently retrieving a huge number of rows.
	 *
	 * <p>
	 * DBvolution detects this situation and, by default, throws a
	 * {@link nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException AccidentalBlankQueryException}
	 * when it happens.
	 *
	 * <p>
	 * To change this behaviour, and allow blank queries, call
	 * {@code setBlankQueriesAllowed(true)}.
	 *
	 * @param allow - TRUE to allow blank queries, FALSE to return it to the
	 * default setting.
	 * @return this DBQuery instance
	 */
	public DBQuery setBlankQueryAllowed(boolean allow) {
		this.options.setBlankQueryAllowed(allow);

		return this;
	}

	/**
	 * Change the Default Setting of Disallowing Accidental Cartesian Joins
	 *
	 * <p>
	 * A common mistake is to create a query without connecting all the tables
	 * in the query and accident retrieve a huge number of rows.
	 *
	 * <p>
	 * DBvolution detects this situation and, by default, throws a
	 * {@link nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException AccidentalCartesianJoinException}
	 * when it happens.
	 *
	 * <p>
	 * To change this behaviour, and allow cartesian joins, call
	 * {@code setCartesianJoinsAllowed(true)}.
	 *
	 * @param allow - TRUE to allow cartesian joins, FALSE to return it to the
	 * default setting.
	 * @return this DBQuery instance
	 */
	public DBQuery setCartesianJoinsAllowed(boolean allow) {
		this.options.setCartesianJoinAllowed(allow);

		return this;
	}

	/**
	 * Constructs the SQL for this DBQuery and executes it on the database,
	 * returning the rows found.
	 *
	 * <p>
	 * Like {@link #getAllRows() getAllRows()} this method retrieves all the
	 * rows for this DBQuery. However it checks the number of rows retrieved and
	 * throws a {@link UnexpectedNumberOfRowsException} if the number of rows
	 * retrieved differs from the expected number.
	 *
	 * <p>
	 * Adds all required DBRows as inner join tables and all optional DBRow as
	 * outer join tables.
	 * <p>
	 * Uses the defined
	 * {@link nz.co.gregs.dbvolution.annotations.DBForeignKey foreign keys} on
	 * the DBRow and
	 * {@link nz.co.gregs.dbvolution.DBRow#addRelationship(nz.co.gregs.dbvolution.datatypes.QueryableDatatype, nz.co.gregs.dbvolution.DBRow, nz.co.gregs.dbvolution.datatypes.QueryableDatatype) added relationships}
	 * to connect the tables. Foreign keys that have been
	 * {@link nz.co.gregs.dbvolution.DBRow#ignoreForeignKey(java.lang.Object) ignored}
	 * are not used.
	 * <p>
	 * Criteria such as
	 * {@link DBNumber#permittedValues(java.lang.Number...)  permitted values}
	 * defined on the fields of the DBRow examples are added as part of the
	 * WHERE clause.
	 *
	 * <p>
	 * Similarly conditions added to the DBQuery using
	 * {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) addCondition}
	 * are added.
	 *
	 * @param expectedRows - the number of rows expected to be retrieved
	 * @return A List of DBQueryRows containing all the DBRow instances aligned
	 * with their related instances.
	 * @throws UnexpectedNumberOfRowsException
	 * @throws SQLException
	 * @see #getAllRows()
	 */
	public List<DBQueryRow> getAllRows(long expectedRows) throws UnexpectedNumberOfRowsException, SQLException {
		List<DBQueryRow> allRows = getAllRows();
		if (allRows.size() != expectedRows) {
			throw new UnexpectedNumberOfRowsException(expectedRows, allRows.size());
		} else {
			return allRows;
		}
	}

	/**
	 * Returns the current setting for ANSI join syntax.
	 *
	 * <p>
	 * Indicates whether or not this query will use JOIN in the FROM clause or
	 * treat foreign keys as a constraint in the WHERE clause.
	 *
	 * <p>
	 * N.B. Optional (outer) tables are only supported with ANSI syntax.
	 *
	 * @return the useANSISyntax flag
	 */
	public boolean isUseANSISyntax() {
		return options.isUseANSISyntax();
	}

	/**
	 * Sets whether this DBQuery will use ANSI syntax.
	 *
	 * <p>
	 * The default is to use ANSI syntax.
	 *
	 * <p>
	 * You should probably use ANSI syntax.
	 *
	 * <p>
	 * ANSI syntax has the foreign key and added relationships defined in the
	 * FROM clause with the JOIN operator. Pre-ANSI syntax treated the foreign
	 * keys and other relationships as part of the WHERE clause.
	 *
	 * <p>
	 * ANSI syntax supports OUTER joins with a standard syntax, and DBvolution
	 * supports OUTER thru the ANSI syntax.
	 *
	 * @param useANSISyntax the useANSISyntax flag to set
	 * @return this DBQuery instance
	 */
	public DBQuery setUseANSISyntax(boolean useANSISyntax) {
		this.options.setUseANSISyntax(useANSISyntax);

		return this;
	}

	/**
	 * Search the classpath and add any DBRow classes that reference the DBRows
	 * within this DBQuery
	 *
	 * <p>
	 * This method automatically enlarges the query by finding all associated
	 * DBRow classes and adding them to the query.
	 *
	 * <p>
	 * In a sense this expands the query out by one level of indirection.
	 *
	 * <p>
	 * N.B. for any realistic database, repeatedly calling this method will
	 * quickly make the query impossibly large.
	 *
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @return this DBQuery instance
	 */
	public DBQuery addAllRelatedTables() throws InstantiationException, IllegalAccessException {
		List<DBRow> tablesToAdd = new ArrayList<DBRow>();
		for (DBRow table : allQueryTables) {
			List<Class<? extends DBRow>> allRelatedTables = table.getAllRelatedTables();
			for (Class<? extends DBRow> relatedTable : allRelatedTables) {
				tablesToAdd.add(relatedTable.newInstance());
			}
		}
		add(tablesToAdd.toArray(new DBRow[]{}));

		return this;
	}

	/**
	 * Search the classpath and add, as optional, any DBRow classes that
	 * reference the DBRows within this DBQuery
	 *
	 * <p>
	 * This method automatically enlarges the query by finding all associated
	 * DBRow classes and adding them to the query as optional tables.
	 *
	 * <p>
	 * In a sense this expands the query out by one level of indirection.
	 *
	 * <p>
	 * N.B. for any realistic database, repeatedly calling this method will
	 * quickly make the query impossibly large.
	 *
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @return this DBQuery instance
	 */
	public DBQuery addAllRelatedTablesAsOptional() throws InstantiationException, IllegalAccessException {
		Set<DBRow> tablesToAdd = new HashSet<DBRow>();
		List<Class<DBRow>> alreadyAddedClasses = new ArrayList<Class<DBRow>>();
		for (DBRow table : allQueryTables) {
			@SuppressWarnings("unchecked")
			Class<DBRow> aClass = (Class<DBRow>) table.getClass();
			alreadyAddedClasses.add(aClass);
		}
		for (DBRow table : allQueryTables) {
			List<Class<? extends DBRow>> allRelatedTables = table.getAllRelatedTables();
			for (Class<? extends DBRow> relatedTable : allRelatedTables) {
				DBRow newInstance = relatedTable.newInstance();
				@SuppressWarnings("unchecked")
				final Class<DBRow> newInstanceClass = (Class<DBRow>) newInstance.getClass();
				if (!alreadyAddedClasses.contains(newInstanceClass)) {
					tablesToAdd.add(newInstance);
					alreadyAddedClasses.add(newInstanceClass);
				}
			}
		}
		addOptional(tablesToAdd.toArray(new DBRow[]{}));

		return this;
	}

	/**
	 * Provides all the DBQueryRow that the instance provided is part of.
	 *
	 * <p>
	 * This method returns the subset of this DBQuery's results that include the
	 * provided instance.
	 *
	 * <p>
	 * Slicing the results like this allows you to get a list of, for instance,
	 * status table DBRows and then process the DBQueryRows that have each
	 * status DBRow as a block.
	 *
	 * @param instance the DBRow instance you are interested in.
	 * @return A list of DBQueryRow instances that relate to the exemplar
	 * @throws SQLException
	 */
	public List<DBQueryRow> getAllRowsContaining(DBRow instance) throws SQLException {
		if (this.needsResults()) {
			getAllRows();
		}
		List<DBQueryRow> returnList = new ArrayList<DBQueryRow>();
		for (DBQueryRow row : results) {
			if (row.get(instance) == instance) {
				returnList.add(row);
			}
		}
		return returnList;
	}

	/**
	 * SOON TO BE REMOVED
	 *
	 * <p>
	 * Please use
	 * {@link #addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression) addCondition}
	 * instead.
	 *
	 * @param leftHandSide
	 * @param operatorWithRightHandSideValues
	 */
//    @Deprecated
	public void addComparison(DBExpression leftHandSide, DBOperator operatorWithRightHandSideValues) {
		comparisons.add(new DBDataComparison(leftHandSide, operatorWithRightHandSideValues));
		results = null;
	}

	/**
	 * Use this method to add complex conditions to the DBQuery.
	 *
	 * <p>
	 * This method takes a BooleanExpression and adds it to the where clause of
	 * the Query
	 *
	 * <p>
	 * The easiest way to get a BooleanExpression is the DBRow.column() method
	 * and then apply the functions you require until you get a
	 * BooleanExpression back.
	 *
	 * <p>
	 * StringExpression, NumberExpression, DateExpression, and BooleanExpression
	 * all provide methods that will help. In particular they have the value()
	 * method to convert base Java types to expressions.
	 *
	 * <p>
	 * Standard uses of this method are:
	 * <pre>
	 * addCondition(myRow.column(myRow.myColumn).like("%THis%"));
	 * addCondition(myRow.column(myRow.myNumber).cos().greaterThan(0.5));
	 * addCondition(StringExpression.value("THis").like(myRwo.column(myRow.myColumn)));
	 * addCondition(BooleanExpression.anyOf(
	 * myRow.column(myRow.myColumn).between("That", "This"),
	 * myRow.column(myRow.myColumn).is("Something"))
	 * );
	 * </pre>
	 *
	 * @param condition a boolean expression that defines a require limit on the
	 * results of the query
	 * @return this DBQuery instance
	 */
	public DBQuery addCondition(BooleanExpression condition) {
		expressions.add(condition);
		results = null;
		return this;
	}

	/**
	 * Set the query to return rows that match any conditions
	 *
	 * <p>
	 * This means that all permitted*, excluded*, and comparisons are optional
	 * for any rows and rows will be returned if they match any of the
	 * conditions.
	 *
	 * <p>
	 * The conditions will be connected by OR in the SQL.
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery setToMatchAnyCondition() {
		options.setMatchAny();
		results = null;
		return this;
	}

	/**
	 * Set the query to only return rows that match all conditions
	 *
	 * <p>
	 * This is the default state
	 *
	 * <p>
	 * This means that all permitted*, excluded*, and comparisons are required
	 * for any rows and the conditions will be connected by AND.
	 *
	 * @return this DBQuery instance
	 */
	public DBQuery setToMatchAllConditions() {
		options.setMatchAll();
		results = null;
		return this;
	}

	/**
	 * Automatically adds the example as a required table if it has criteria, or
	 * as an optional table otherwise.
	 *
	 * <p>
	 * Any DBRow example passed to this method that has criteria specified on
	 * it, however vague, will become a required table on the query.
	 *
	 * <p>
	 * Any DBRow example that has no criteria, i.e. where {@link DBRow#willCreateBlankQuery(nz.co.gregs.dbvolution.DBDatabase)
	 * } is TRUE, will be added as an optional table.
	 *
	 * <p>
	 * Warning: not specifying a required table will result in a FULL OUTER join
	 * which some database don't handle. You may want to test that the query is
	 * not blank after adding all your tables.
	 *
	 * @param exampleWithOrWithoutCriteria an example DBRow that should be added
	 * to the query as a required or optional table as appropriate.
	 * @return this DBQuery instance
	 */
	public DBQuery addOptionalIfNonspecific(DBRow exampleWithOrWithoutCriteria) {
		if (exampleWithOrWithoutCriteria.willCreateBlankQuery(database)) {
			addOptional(exampleWithOrWithoutCriteria);
		} else {
			add(exampleWithOrWithoutCriteria);
		}
		return this;
	}

	/**
	 * Used by DBReport to add columns to the SELECT clause
	 *
	 * @param identifyingObject
	 * @param expressionToAdd
	 * @return this DBQuery instance
	 */
	public DBQuery addExpressionColumn(Object identifyingObject, DBExpression expressionToAdd) {
		expressionColumns.put(identifyingObject, expressionToAdd);
		return this;
	}

	/**
	 * Used by DBReport to add columns to the GROUP BY clause.
	 *
	 * @param identifyingObject
	 * @param expressionToAdd
	 * @return this DBQuery instance
	 */
	public DBQuery addGroupByColumn(Object identifyingObject, DBExpression expressionToAdd) {
		groupByColumns.put(identifyingObject, expressionToAdd);
		return this;
	}

	protected void refreshQuery() {
		results = null;
	}

	void setRawSQL(String rawQuery) {
		if (rawQuery == null) {
			this.rawSQLClause = "";
		} else {
			this.rawSQLClause = " " + rawQuery + " ";
		}
	}

	/**
	 * Adds Extra Examples to the Query.
	 *
	 * <p>
	 * The included DBRow instances will be used to add extra criteria as though
	 * they were an added table.
	 *
	 * <p>
	 * Only useful for DBReports or queries that have been
	 * {@link DBQuery#setToMatchAnyCondition() set to match any of the conditions}.
	 *
	 * <p>
	 * They will NOT be added as tables however, for that use
	 * {@link #add(nz.co.gregs.dbvolution.DBRow...) add and related methods}.
	 *
	 * @param extraExamples
	 */
	void addExtraExamples(DBRow... extraExamples) {
		this.extraExamples.addAll(Arrays.asList(extraExamples));
	}

	/**
	 * Show the Graph window of the current QueryGraph.
	 *
	 * <p>
	 * A pictorial representation to help you with diagnosing the issues with
	 * queries and to visualize what is actually being used by DBvolution.
	 *
	 * <p>
	 * Internally DBvolution uses a graph to design the query that will be used.
	 * This graph is helpful for visualizing the underlying query, more so than
	 * an SQL query dump. So this method will display the query graph of this
	 * query at this time. The graph cannot be altered through the window but it
	 * can be moved to help show the parts of the graph. You can manipulate the
	 * query graph by
	 * {@link DBQuery#add(nz.co.gregs.dbvolution.DBRow[])  adding tables}, {@link DBRow#addRelationship(nz.co.gregs.dbvolution.datatypes.QueryableDatatype, nz.co.gregs.dbvolution.DBRow, nz.co.gregs.dbvolution.datatypes.QueryableDatatype)  adding relationships to the DBRow}
	 * instances, or
	 * {@link DBRow#ignoreForeignKey(java.lang.Object) ignoring inappropriate foreign keys}.
	 *
	 */
	public void displayQueryGraph() {
		initialiseQueryGraph();

		edu.uci.ics.jung.graph.Graph<QueryGraph.QueryGraphNode, DBRelationship> jungGraph = queryGraph.getJungGraph();

		Layout<QueryGraph.QueryGraphNode, DBRelationship> layout = new FRLayout<QueryGraph.QueryGraphNode, DBRelationship>(jungGraph);
		layout.setSize(new Dimension(550, 400));

		VisualizationViewer<QueryGraph.QueryGraphNode, DBRelationship> vv = new VisualizationViewer<QueryGraph.QueryGraphNode, DBRelationship>(layout);
		vv.setPreferredSize(new Dimension(600, 480));

		DefaultModalGraphMouse<QueryGraph.QueryGraphNode, String> gm = new DefaultModalGraphMouse<QueryGraph.QueryGraphNode, String>();
		gm.setMode(ModalGraphMouse.Mode.PICKING);
		vv.setGraphMouse(gm);

		RenderContext<QueryGraph.QueryGraphNode, DBRelationship> renderContext = vv.getRenderContext();
		renderContext.setEdgeLabelTransformer(new ToStringLabeller<DBRelationship>());
		renderContext.setVertexLabelTransformer(new ToStringLabeller<QueryGraph.QueryGraphNode>());
		renderContext.setEdgeLabelRenderer(new DefaultEdgeLabelRenderer(Color.yellow, false));

		JFrame frame = new JFrame("DBQuery Graph");
		frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		frame.setResizable(true);
		frame.getContentPane().add(vv);
		frame.pack();
		frame.setVisible(true);
	}
//    public void displayQueryGraphStream() {
//        initialiseQueryGraph();
//        queryGraph.getGraphStream().display();
//    }
//
//    public Graph getQueryGraphDisplay() {
//        initialiseQueryGraph();
//        return queryGraph.getGraphStream();
//    }

	private void initialiseQueryGraph() {
		if (queryGraph == null) {
			queryGraph = new QueryGraph(database, allQueryTables, expressions, options);
		} else {
			queryGraph.clear();
			queryGraph.addAndConnectToRelevant(database, allQueryTables, expressions, options);
		}
	}
}
