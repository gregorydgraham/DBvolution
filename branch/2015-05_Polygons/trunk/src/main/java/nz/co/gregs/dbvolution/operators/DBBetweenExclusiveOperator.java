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
package nz.co.gregs.dbvolution.operators;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.DateResult;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.NumberResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.expressions.StringResult;

/**
 *
 * @author Gregory Graham
 */
public class DBBetweenExclusiveOperator extends DBOperator {

	private static final long serialVersionUID = 1L;
//	DBExpression lowest;
//	DBExpression highest;

	public DBBetweenExclusiveOperator(DBExpression lowValue, DBExpression highValue) {
		super();
		firstValue = lowValue == null ? lowValue : lowValue.copy();
		secondValue = highValue == null ? highValue : highValue.copy();
	}

	@Override
	public DBBetweenExclusiveOperator copyAndAdapt(QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor typeAdaptor) {
		DBBetweenExclusiveOperator op = new DBBetweenExclusiveOperator(typeAdaptor.convert(firstValue), typeAdaptor.convert(secondValue));
		op.invertOperator = this.invertOperator;
		op.includeNulls = this.includeNulls;
		return op;
	}

	@Override
	public BooleanExpression generateWhereExpression(DBDatabase db, DBExpression column) {
		DBExpression genericExpression = column;
		BooleanExpression betweenOp = BooleanExpression.trueExpression();
		if (genericExpression instanceof StringExpression) {
			StringExpression stringExpression = (StringExpression) genericExpression;
			StringResult firstStringExpr = null;
			StringResult secondStringExpr = null;
			if (firstValue instanceof NumberResult) {
				NumberResult numberResult = (NumberResult) firstValue;
				firstStringExpr = new NumberExpression(numberResult).stringResult();
			} else if (firstValue instanceof StringResult) {
				firstStringExpr = (StringResult) firstValue;
			}
			if (secondValue instanceof NumberResult) {
				NumberResult numberResult = (NumberResult) secondValue;
				secondStringExpr = new NumberExpression(numberResult).stringResult();
			} else if (secondValue instanceof StringResult) {
				secondStringExpr = (StringResult) secondValue;
			}
			if (firstStringExpr != null && secondStringExpr != null) {
				betweenOp = stringExpression.bracket().isBetweenExclusive(firstStringExpr, secondStringExpr);
			}
		} else if ((genericExpression instanceof NumberExpression)
				&& (firstValue instanceof NumberResult)
				&& (secondValue instanceof NumberResult)) {
			NumberExpression numberExpression = (NumberExpression) genericExpression;
			betweenOp = numberExpression.isBetweenExclusive((NumberResult) firstValue, (NumberResult) secondValue);
		} else if ((genericExpression instanceof DateExpression)
				&& (firstValue instanceof DateResult)
				&& (secondValue instanceof DateResult)) {
			DateExpression dateExpression = (DateExpression) genericExpression;
			betweenOp = dateExpression.isBetweenExclusive((DateResult) firstValue, (DateResult) secondValue);
		} else {
			throw new DBRuntimeException("whoops");
		}
		return this.invertOperator ? betweenOp.not() : betweenOp;
	}
}
