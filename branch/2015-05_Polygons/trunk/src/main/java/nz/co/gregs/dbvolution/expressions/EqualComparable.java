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

/**
 * Indicates that the class can be compared to other instances of this class as
 * if the instances were equivalent.
 *
 * <p>
 * EqualsComparable expressions must have an equivalent to the EQUALS (=)
 * operation.
 *
 * @author Gregory Graham
 * @param <A> the class that can be compared using the "=" operator
 *
 */
public interface EqualComparable<A> {

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * EQUALS operation.
	 *
	 * @param anotherInstance
	 * @return a BooleanExpression
	 */
	public BooleanExpression is(A anotherInstance);
}
