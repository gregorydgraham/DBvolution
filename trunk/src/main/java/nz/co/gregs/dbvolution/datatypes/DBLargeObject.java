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
package nz.co.gregs.dbvolution.datatypes;

import java.io.InputStream;
import nz.co.gregs.dbvolution.results.LargeObjectResult;

/**
 * Encapsulates database values that are large, vague objects.
 *
 * <p>
 * Use a DBLargeObject subtype when the column is a {@code BLOB}, {@code CLOB},
 * {@code TEXT}, {@code JavaObject}, or similar datatype.
 *
 * <p>
 * Mostly you should use {@link  DBByteArray} though there should be other more
 * specific classes eventually. There is also {@link DBJavaObject} for storing
 * Java objects directly in the database.
 *
 * @author Gregory Graham
 */
public abstract class DBLargeObject extends QueryableDatatype implements LargeObjectResult {

	private static final long serialVersionUID = 1L;

	/**
	 * The default constructor for DBLargeObject.
	 *
	 * <p>
	 * Creates an unset undefined DBLargeObject object.
	 *
	 */
	public DBLargeObject() {
		super();
	}

	/**
	 * The column expression constructor for DBLargeObject.
	 *
	 * <p>
	 * Creates DBLargeObject that will be generated by the database at query time.
	 *
	 * @param blobResult the expression to be evaluated during the query
	 */
	public DBLargeObject(LargeObjectResult blobResult) {
		super(blobResult);
	}

	/**
	 * Returns the internal InputStream.
	 *
	 * @return an InputStream to read the bytes.
	 */
	public abstract InputStream getInputStream();

	/**
	 *
	 * @return the size of the Large Object as an int
	 */
	public abstract int getSize();

	@Override
	public String toString() {
		return "/*BINARY DATA*/";
	}

	@Override
	public DBLargeObject copy() {
		return (DBLargeObject) super.copy();
	}

}
