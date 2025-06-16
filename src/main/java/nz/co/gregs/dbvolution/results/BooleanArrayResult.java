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
package nz.co.gregs.dbvolution.results;

import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 * Interface required to be implemented by all DBExpressions that produce BIT
 * results
 *
 * <p>
 * DBvolution attempts to maintain type safety using the *Result interfaces.
 * Most operations requiring a bit type will not accept anything other than an
 * actual bit type.
 *
 * <p>
 * Add {@code implements BooleanArrayResult} to your class and override the copy
 * method so that it returns your class type.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @see DBExpression
 */
public interface BooleanArrayResult extends EqualResult<Boolean[]> {

	@Override
	public BooleanArrayResult copy();
}
