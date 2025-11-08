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
package nz.co.gregs.dbvolution.operators;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBEnum;
import nz.co.gregs.dbvolution.datatypes.DBIntegerEnum;
import nz.co.gregs.dbvolution.datatypes.DBStringEnum;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.ComparisonBetweenTwoDissimilarTypes;
import nz.co.gregs.dbvolution.expressions.BooleanArrayExpression;
import nz.co.gregs.dbvolution.results.BooleanArrayResult;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.results.BooleanResult;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.results.DateResult;
import nz.co.gregs.dbvolution.results.EqualComparable;
import nz.co.gregs.dbvolution.expressions.spatial2D.Polygon2DExpression;
import nz.co.gregs.dbvolution.results.Polygon2DResult;
import nz.co.gregs.dbvolution.expressions.DateRepeatExpression;
import nz.co.gregs.dbvolution.expressions.EqualExpression;
import nz.co.gregs.dbvolution.expressions.InstantExpression;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.results.DateRepeatResult;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.RangeExpression;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.InstantResult;
import nz.co.gregs.dbvolution.results.IntegerResult;
import nz.co.gregs.dbvolution.results.StringResult;

/**
 * Implements the EQUALS operator.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBEqualsOperator extends DBOperator {

	private static final long serialVersionUID = 1L;

	/**
	 * Implements the EQUALS operator.
	 *
	 * @param equalTo the expression to compare to
	 */
	@SuppressFBWarnings(
			value = "NP_LOAD_OF_KNOWN_NULL_VALUE",
			justification = "Null is a valid value in databases")
	public DBEqualsOperator(DBExpression equalTo) {
		super(equalTo == null ? equalTo : equalTo.copy());
	}

	/**
	 * Implements the EQUALS operator.
	 * 
	 * <p>Probably not the right method to use.</p>
	 *
	 * @param equalTo the expression to compare to.
	 */
	public DBEqualsOperator(Object equalTo) {
		super(QueryableDatatype.getQueryableDatatypeForObject(equalTo));
	}

	@Override
	public DBEqualsOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
		DBEqualsOperator op = new DBEqualsOperator(typeAdaptor.convert(getFirstValue()));
		op.invertOperator = this.invertOperator;
		op.includeNulls = this.includeNulls;
		return op;
	}

	@Override
	@SuppressWarnings("unchecked")
	public BooleanExpression generateWhereExpression(DBDefinition db, DBExpression column) throws ComparisonBetweenTwoDissimilarTypes {
		DBExpression genericExpression = column;
		BooleanExpression op = BooleanExpression.trueExpression();
		if (genericExpression instanceof EqualComparable) {
      final DBExpression firstValue = getFirstValue();
			try {
				EqualComparable<Object, DBExpression> columnEqual = (EqualComparable<Object, DBExpression>) genericExpression;
				if (invertOperator) {
					op = columnEqual.isNot(firstValue);
				} else {
					op = columnEqual.is(firstValue);
				}
				return op;
			} catch (Exception exp) {
				if (genericExpression instanceof StringExpression) {
					StringExpression stringExpression = (StringExpression) genericExpression;
					if ((firstValue instanceof StringResult) || firstValue == null) {
						op = stringExpression.bracket().is((StringResult) firstValue);
					} else if (firstValue instanceof DBStringEnum) {
						op = stringExpression.bracket().is(((DBStringEnum) firstValue).getValue());
					} else if (firstValue instanceof NumberResult) {
						op = stringExpression.bracket().is(new NumberExpression((NumberResult) firstValue).stringResult());
					} else if (firstValue instanceof IntegerResult) {
						op = stringExpression.bracket().is(new IntegerExpression((IntegerResult) firstValue).stringResult());
					} else if (firstValue instanceof DBEnum) {
            final DBEnum<?,?> firstValueEnum = (DBEnum) firstValue;
						op = stringExpression.bracket().is(firstValueEnum.stringValue());
					} else {
						throw new nz.co.gregs.dbvolution.exceptions.ComparisonBetweenTwoDissimilarTypes(db, genericExpression, firstValue);
					}
				} else if ((genericExpression instanceof NumberExpression) && ((firstValue instanceof NumberResult) || firstValue == null)) {
					NumberExpression numberExpression = (NumberExpression) genericExpression;
					op = numberExpression.is((NumberResult) firstValue);
				} else if ((genericExpression instanceof NumberExpression) && ((firstValue instanceof IntegerResult))) {
					NumberExpression numberExpression = (NumberExpression) genericExpression;
					op = numberExpression.is(new IntegerExpression((IntegerResult) firstValue).numberResult());
				} else if ((genericExpression instanceof NumberExpression) && ((firstValue instanceof DBIntegerEnum))) {
					NumberExpression numberExpression = (NumberExpression) genericExpression;
					op = numberExpression.is(((DBIntegerEnum) firstValue).numberValue());
				} else if ((genericExpression instanceof IntegerExpression) && ((firstValue instanceof IntegerResult) || firstValue == null)) {
					IntegerExpression integerExpression = (IntegerExpression) genericExpression;
					op = integerExpression.is((IntegerResult) firstValue);
				} else if ((genericExpression instanceof IntegerExpression) && ((firstValue instanceof NumberResult) || firstValue == null)) {
					IntegerExpression integerExpression = (IntegerExpression) genericExpression;
					op = integerExpression.numberResult().is((NumberResult) firstValue);
				} else if ((genericExpression instanceof DateExpression) && ((firstValue instanceof DateResult) || firstValue == null)) {
					DateExpression dateExpression = (DateExpression) genericExpression;
					op = dateExpression.is((DateResult) firstValue);
				} else if ((genericExpression instanceof BooleanExpression) && ((firstValue instanceof BooleanResult) || firstValue == null)) {
					BooleanExpression boolExpr = (BooleanExpression) genericExpression;
					op = boolExpr.is((BooleanResult) firstValue);
				} else if ((genericExpression instanceof BooleanArrayExpression) && ((firstValue instanceof BooleanArrayResult) || firstValue == null)) {
					BooleanArrayExpression boolExpr = (BooleanArrayExpression) genericExpression;
					op = boolExpr.is((BooleanArrayResult) firstValue);
				} else if ((genericExpression instanceof DateRepeatExpression) && ((firstValue instanceof DateRepeatResult) || firstValue == null)) {
					DateRepeatExpression intervalExpr = (DateRepeatExpression) genericExpression;
					op = intervalExpr.is((DateRepeatResult) firstValue);
				} else if ((genericExpression instanceof Polygon2DExpression) && ((firstValue instanceof Polygon2DResult) || firstValue == null)) {
					Polygon2DExpression intervalExpr = (Polygon2DExpression) genericExpression;
					op = intervalExpr.is((Polygon2DResult) firstValue);
				} else if (genericExpression instanceof EqualExpression) {
					if (genericExpression.getQueryableDatatypeForExpressionValue().getClass().equals(firstValue.getQueryableDatatypeForExpressionValue().getClass())) {
						EqualExpression dateExpression = (EqualExpression) genericExpression;
						op = dateExpression.is(firstValue);
					} else {
						throw new nz.co.gregs.dbvolution.exceptions.ComparisonBetweenTwoDissimilarTypes(db, genericExpression, firstValue);
					}
				} else {
					throw new nz.co.gregs.dbvolution.exceptions.ComparisonBetweenTwoDissimilarTypes(db, genericExpression, firstValue);
				}
				return this.invertOperator ? op.not() : op;
			}
		} else {
			throw new nz.co.gregs.dbvolution.exceptions.IncomparableTypeUsedInComparison(db, genericExpression);
		}
	}
}
