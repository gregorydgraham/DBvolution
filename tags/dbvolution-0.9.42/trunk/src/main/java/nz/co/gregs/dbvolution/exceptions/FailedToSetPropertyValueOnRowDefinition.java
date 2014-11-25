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
package nz.co.gregs.dbvolution.exceptions;

/**
 * Usually thrown when 'target' isn't of the same type as 'field' is declared
 * on.
 *
 * @author gregory.graham
 */
public class FailedToSetPropertyValueOnRowDefinition extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when the internal field cannot be read correctly.
	 *
	 * <p>
	 * Apparently this should never happen.
	 *
	 * @param qualifiedName
	 * @param className
	 * @param cause
	 */
	public FailedToSetPropertyValueOnRowDefinition(String qualifiedName, String className, Throwable cause) {
		super("Internal error reading field "
				+ qualifiedName + " on object of type "
				+ className + " (this is probably a DBvolution bug): "
				+ cause.getLocalizedMessage(), cause);
	}
}
