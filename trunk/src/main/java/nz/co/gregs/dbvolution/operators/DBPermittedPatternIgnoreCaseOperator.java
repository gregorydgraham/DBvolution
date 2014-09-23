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
package nz.co.gregs.dbvolution.operators;

import nz.co.gregs.dbvolution.expressions.StringExpression;

/**
 *
 * @author gregory.graham
 */
public class DBPermittedPatternIgnoreCaseOperator extends DBLikeCaseInsensitiveOperator {

	public static final long serialVersionUID = 1L;

	public DBPermittedPatternIgnoreCaseOperator(String likeableValue) {
		super(new StringExpression(likeableValue));
	}

	public DBPermittedPatternIgnoreCaseOperator(StringExpression likeableValue) {
		super(likeableValue);
	}

	public DBPermittedPatternIgnoreCaseOperator() {
	}
}
