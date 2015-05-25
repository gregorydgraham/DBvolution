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

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 *
 * @author Gregory Graham
 */
public class DBIsNullOperator extends DBOperator {

	private static final long serialVersionUID = 1L;

	@Override
	public DBIsNullOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
		return this;
	}

	@Override
	public BooleanExpression generateWhereExpression(DBDatabase db, DBExpression column) {
		DBExpression genericExpression = column;
		return this.invertOperator ? BooleanExpression.isNotNull(genericExpression) : BooleanExpression.isNull(genericExpression);
	}
}
