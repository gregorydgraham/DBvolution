/*
 * Copyright 2015 gregorygraham.
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
import nz.co.gregs.dbvolution.datatypes.DBByteArray;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer;
import nz.co.gregs.dbvolution.exceptions.ComparisonBetweenTwoDissimilarTypes;
import nz.co.gregs.dbvolution.exceptions.IncomparableTypeUsedInComparison;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.LargeObjectExpression;
import nz.co.gregs.dbvolution.results.LargeObjectResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author gregorygraham
 */
public class DBEqualsOperatorTest extends AbstractTest {

	public DBEqualsOperatorTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test(expected = ComparisonBetweenTwoDissimilarTypes.class)
	public void testGenerateWhereExpressionThrowsDissimilarComparisonExceptionDateAndString() {
		System.out.println("generateWhereExpression");
		DBExpression column = new StringExpression("a string");
		DBEqualsOperator instance = new DBEqualsOperator(new DateExpression(april2nd2011));
		BooleanExpression result = instance.generateWhereExpression(database, column);
	}

	@Test(expected = ComparisonBetweenTwoDissimilarTypes.class)
	public void testGenerateWhereExpressionThrowsDissimilarComparisonExceptionStringAndDate() {
		System.out.println("generateWhereExpression");
		DBExpression column = new DateExpression(april2nd2011);
		DBEqualsOperator instance = new DBEqualsOperator(new StringExpression("a string"));
		BooleanExpression result = instance.generateWhereExpression(database, column);
	}

	@Test(expected = IncomparableTypeUsedInComparison.class)
	public void testGenerateWhereExpressionThrowsDissimilarComparisonExceptionNotEqualsComparable() {
		System.out.println("generateWhereExpression");
		DBExpression column = new LargeObjectExpression(new DBByteArray());
		DBEqualsOperator instance = new DBEqualsOperator(new StringExpression("a string"));
		BooleanExpression result = instance.generateWhereExpression(database, column);
	}

}
