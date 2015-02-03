/*
 * Copyright 2014 gregory.graham.
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
package nz.co.gregs.dbvolution.expressions;

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBBooleanArray;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * The Expression object for bit array columns.
 *
 * @author gregory.graham
 */
// TODO imlement EqualsComparable<BooleanArrayExpression>
public class BooleanArrayExpression implements BooleanArrayResult, EqualComparable<BooleanArrayResult> {

	private final BooleanArrayResult innerBooleanArrayResult;

	/**
	 * Default Constructor.
	 */
	protected BooleanArrayExpression() {
		this.innerBooleanArrayResult = new DBBooleanArray();
	}

	/**
	 * Create a BitsExpression from an existing BitResult object.
	 *
	 * @param bitResult	bitResult
	 */
	public BooleanArrayExpression(BooleanArrayResult bitResult) {
		this.innerBooleanArrayResult = bitResult;
	}

	@Override
	public BooleanArrayExpression copy() {
		return new BooleanArrayExpression(this.getInnerBooleanArrayResult());
	}

	@Override
	public QueryableDatatype getQueryableDatatypeForExpressionValue() {
		return new DBBooleanArray();
	}

	@Override
	public String toSQLString(DBDatabase db) {
		if (innerBooleanArrayResult != null) {
			return innerBooleanArrayResult.toSQLString(db);
		} else {
			return "";
		}
	}

	@Override
	public boolean isAggregator() {
		if (innerBooleanArrayResult != null) {
			return innerBooleanArrayResult.isAggregator();
		} else {
			return false;
		}
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		if (innerBooleanArrayResult != null) {
			return innerBooleanArrayResult.getTablesInvolved();
		} else {
			return new HashSet<DBRow>();
		}
	}

	@Override
	public boolean getIncludesNull() {
		if (innerBooleanArrayResult != null) {
			return innerBooleanArrayResult.getIncludesNull();
		} else {
			return false;
		}
	}

	@Override
	public boolean isPurelyFunctional() {
		if (innerBooleanArrayResult == null) {
			return true;
		} else {
			return innerBooleanArrayResult.isPurelyFunctional();
		}
	}

	/**
	 * Return the BooleanArrayResult held internally in this class.
	 *
	 * @return The BooleanArrayResult used internally.
	 */
	protected BooleanArrayResult getInnerBooleanArrayResult() {
		return innerBooleanArrayResult;
	}

	/**
	 * Create a BooleanExpression that will compare the BooleanArrayResult
	 * provided to this BooleanArrayExpression using the equivalent of the
	 * EQUALS operator.
	 *
	 * @param i the value to compare this expression to
	 * @return a BooleanExpresson of the Bit comparison of the number and this
	 * expression.
	 */
	public BooleanExpression is(BooleanArrayResult i) {
		return new BooleanExpression(new DBBinaryBooleanArithmetic(this, i) {
			@Override
			protected String getEquationOperator(DBDatabase db) {
				return " = ";
			}
		});
	}

	private static abstract class DBBinaryBooleanArithmetic extends BooleanExpression {

		private BooleanArrayExpression first;
		private BooleanArrayResult second;
		private boolean requiresNullProtection;

		DBBinaryBooleanArithmetic(BooleanArrayExpression first, BooleanArrayResult second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		@Override
		public DBBoolean getQueryableDatatypeForExpressionValue() {
			return new DBBoolean();
		}

		@Override
		public String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return "("+BooleanExpression.isNull(first).toSQLString(db)+")";
			} else {
				return "("+first.toSQLString(db) + this.getEquationOperator(db) + second.toSQLString(db)+")";
			}
		}

		@Override
		public DBBinaryBooleanArithmetic copy() {
			DBBinaryBooleanArithmetic newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String getEquationOperator(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
			if (second != null) {
				hashSet.addAll(second.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}

		@Override
		public boolean isPurelyFunctional() {
			if (first == null && second == null) {
				return true;
			} else {
				return first.isPurelyFunctional() && second.isPurelyFunctional();
			}
		}
	}

}
