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

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.logging.*;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.expressions.LargeObjectExpression;
import org.apache.commons.codec.binary.Base64;

/**
 *
 * Implements the abstractions necessary to handle arbitrary byte streams and
 * files stored in the database.
 *
 * <p>
 * Use DBByteArray for files, streams, and exceptionally long text. Store Java
 * instances/objects as {@link DBJavaObject} for greater convenience.
 *
 * <p>
 * Generally DBBoolean is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBByteArray myBoolColumn = new DBByteArray();}
 *
 * <p>
 * DBByteArray is the standard type of {@link DBLargeObject BLOB columns}.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBByteArray extends DBLargeObject {

	private static final long serialVersionUID = 1;
	transient BufferedInputStream byteStream = null;

	/**
	 * The Default constructor for a DBByteArray.
	 *
	 */
	public DBByteArray() {
		super();
	}

	/**
	 * Creates a column expression with a large object result from the expression
	 * provided.
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param aThis an expression that will result in a large object value
	 */
	public DBByteArray(LargeObjectExpression aThis) {
		super(aThis);
	}

	/**
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the standard SQL datatype that corresponds to this QDT as a String
	 */
	@Override
	public String getSQLDatatype() {
		return "BLOB";
	}

	/**
	 * Sets the value of this DBByteArray to the byte array supplied.
	 *
	 * @param byteArray	byteArray
	 */
	public void setValue(byte[] byteArray) {
		super.setLiteralValue(byteArray);
		if (byteArray == null) {
			byteStream = new BufferedInputStream(new ByteArrayInputStream(new byte[]{}));
		} else {
			byteStream = new BufferedInputStream(new ByteArrayInputStream(byteArray));
		}
	}

	/**
	 * Sets the value of this DBByteArray to the InputStream supplied.
	 *
	 * <p>
	 * The input stream will not be read until the containing DBRow is
	 * saved/inserted.
	 *
	 * @param inputViaStream	inputViaStream
	 */
	public void setValue(InputStream inputViaStream) {
		super.setLiteralValue(inputViaStream);
		byteStream = new BufferedInputStream(inputViaStream);
	}

	/**
	 * Sets the value of this DBByteArray to the file supplied.
	 *
	 * <p>
	 * Unlike {@link #setValue(java.io.InputStream) setting an InputStream}, the
	 * file is read immediately and stored internally. If you would prefer to
	 * delay the reading of the file, wrap the file in a {@link FileInputStream}.
	 *
	 * @param fileToRead fileToRead
	 * @throws java.io.IOException java.io.IOException
	 */
	public void setValue(File fileToRead) throws IOException {
		setValue(setFromFileSystem(fileToRead));
	}

	/**
	 * Set the value of the DBByteArray to the String provided.
	 *
	 * @param string	string
	 */
	public void setValue(String string) {
		setValue(string.getBytes());
	}

	@Override
	void setValue(Object newLiteralValue) {
		if (newLiteralValue instanceof byte[]) {
			setValue((byte[]) newLiteralValue);
		} else if (newLiteralValue instanceof DBByteArray) {
			final DBByteArray valBytes = (DBByteArray) newLiteralValue;
			setValue(valBytes.getValue());
		} else if (newLiteralValue instanceof String) {
			final String valBytes = (String) newLiteralValue;
			setValue(valBytes.getBytes());
		} else {
			throw new ClassCastException(this.getClass().getSimpleName() + ".setValue() Called With A Non-Byte[]: Use only byte arrays with this class");
		}
	}

	private byte[] getFromBinaryStream(ResultSet resultSet, String fullColumnName) throws SQLException {
		byte[] bytes = new byte[]{};
		InputStream inputStream;
		inputStream = resultSet.getBinaryStream(fullColumnName);
		if (resultSet.wasNull()) {
			inputStream = null;
		}
		if (inputStream == null) {
			this.setToNull();
		} else {
			bytes = getBytesFromInputStream(inputStream);
		}
		return bytes;
	}

	private byte[] getFromBLOB(ResultSet resultSet, String fullColumnName) throws SQLException {
		byte[] bytes = new byte[]{};
		Blob blob = resultSet.getBlob(fullColumnName);
		if (resultSet.wasNull()) {
			blob = null;
		}
		if (blob == null) {
			this.setToNull();
		} else {
			InputStream inputStream = blob.getBinaryStream();
			bytes = getBytesFromInputStream(inputStream);
		}
		return bytes;
	}

	private byte[] getBytesFromInputStream(InputStream inputStream) {
		InputStream input = new BufferedInputStream(inputStream);
		List<byte[]> byteArrays = new ArrayList<byte[]>();
		try {
			byte[] resultSetBytes;
			final int byteArrayDefaultSize = 100000;
			resultSetBytes = new byte[byteArrayDefaultSize];
			int bytesRead = input.read(resultSetBytes);
			while (bytesRead > 0) {
				if (bytesRead == byteArrayDefaultSize) {
					byteArrays.add(resultSetBytes);
				} else {
					byte[] shortBytes = new byte[bytesRead];
					System.arraycopy(resultSetBytes, 0, shortBytes, 0, bytesRead);
					byteArrays.add(shortBytes);
				}
				resultSetBytes = new byte[byteArrayDefaultSize];
				bytesRead = input.read(resultSetBytes);
			}
		} catch (IOException ex) {
			Logger.getLogger(DBByteArray.class.getName()).log(Level.SEVERE, null, ex);
		}
		return concatAllByteArrays(byteArrays);
	}

	public static byte[] concatAllByteArrays(List<byte[]> bytes) {
		byte[] first = bytes.get(0);
		bytes.remove(0);
		byte[][] rest = bytes.toArray(new byte[][]{});
		int totalLength = first.length;
		for (byte[] array : rest) {
			totalLength += array.length;
		}
		byte[] result = Arrays.copyOf(first, totalLength);
		int offset = first.length;
		for (byte[] array : rest) {
			System.arraycopy(array, 0, result, offset, array.length);
			offset += array.length;
		}
		return result;
	}
	
	private byte[] getFromGetBytes(ResultSet resultSet, String fullColumnName) throws SQLException {
		byte[] bytes = resultSet.getBytes(fullColumnName);
		return bytes;
	}

	private byte[] getFromCharacterReader(ResultSet resultSet, String fullColumnName) throws SQLException, IOException {
		byte[] decodeBuffer = new byte[]{};
		Reader inputReader = null;
		try {
			inputReader = resultSet.getCharacterStream(fullColumnName);
		} catch (NullPointerException nullEx) {
			;// NullPointerException is thrown by a SQLite-JDBC bug sometimes.
		}
		if (inputReader != null) {
			if (resultSet.wasNull()) {
				this.setToNull();
			} else {
				BufferedReader input = new BufferedReader(inputReader);
				List<char[]> byteArrays = new ArrayList<char[]>();
				try {
					char[] resultSetBytes;
					final int byteArrayDefaultSize = 100000;
					resultSetBytes = new char[byteArrayDefaultSize];
					int bytesRead = input.read(resultSetBytes);
					while (bytesRead > 0) {
						if (bytesRead == byteArrayDefaultSize) {
							byteArrays.add(resultSetBytes);
						} else {
							char[] shortBytes = new char[bytesRead];
							System.arraycopy(resultSetBytes, 0, shortBytes, 0, bytesRead);
							byteArrays.add(shortBytes);
						}
						resultSetBytes = new char[byteArrayDefaultSize];
						bytesRead = input.read(resultSetBytes);
					}
				} catch (IOException ex) {
					Logger.getLogger(DBByteArray.class.getName()).log(Level.SEVERE, null, ex);
				}
				char[] bytes = concatAllCharArrays(byteArrays);
				decodeBuffer = Base64.decodeBase64(new String(bytes));
			}
		}
		return decodeBuffer;
	}

	public static char[] concatAllCharArrays(List<char[]> chars) {
		char[] first = chars.get(0);
		chars.remove(0);
		char[][] rest = chars.toArray(new char[][]{});
		int totalLength = first.length;
		for (char[] array : rest) {
			totalLength += array.length;
		}
		char[] result = Arrays.copyOf(first, totalLength);
		int offset = first.length;
		for (char[] array : rest) {
			System.arraycopy(array, 0, result, offset, array.length);
			offset += array.length;
		}
		return result;
	}

	private byte[] getFromCLOB(ResultSet resultSet, String fullColumnName) throws SQLException {
		byte[] bytes = new byte[]{};
		Clob clob = resultSet.getClob(fullColumnName);
		if (resultSet.wasNull() || clob == null) {
			this.setToNull();
		} else {
			final Reader characterStream = clob.getCharacterStream();
			try {
				BufferedReader input = new BufferedReader(characterStream);
				List<byte[]> byteArrays = new ArrayList<byte[]>();
				try {
					char[] resultSetBytes;
					final int byteArrayDefaultSize = 100000;
					resultSetBytes = new char[byteArrayDefaultSize];
					int bytesRead = input.read(resultSetBytes);
					while (bytesRead > 0) {
						if (bytesRead == byteArrayDefaultSize) {
							byteArrays.add(String.valueOf(resultSetBytes).getBytes());
						} else {
							char[] shortBytes = new char[bytesRead];
							System.arraycopy(resultSetBytes, 0, shortBytes, 0, bytesRead);
							byteArrays.add(String.valueOf(shortBytes).getBytes());
						}
						resultSetBytes = new char[byteArrayDefaultSize];
						bytesRead = input.read(resultSetBytes);
					}
				} catch (IOException ex) {
					Logger.getLogger(DBByteArray.class.getName()).log(Level.SEVERE, null, ex);
				}
				bytes = concatAllByteArrays(byteArrays);
			} finally {
				try {
					characterStream.close();
				} catch (IOException ex) {
					Logger.getLogger(DBByteArray.class.getName()).log(Level.SEVERE, null, ex);
				}
			}
		}
		return bytes;
	}

	@Override
	public String formatValueForSQLStatement(DBDatabase db) {
		throw new UnsupportedOperationException("Binary datatypes like " + this.getClass().getSimpleName() + " do not have a simple SQL representation. Do not call getSQLValue(), use the getInputStream() method instead.");
	}

	/**
	 * Tries to set the DBDyteArray to the contents of the supplied file.
	 *
	 * <p>
	 * Convenience method for {@link #setFromFileSystem(java.io.File) }.
	 *
	 * @param originalFile	originalFile
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the byte[] of the contents of the file.
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 *
	 *
	 */
	public byte[] setFromFileSystem(String originalFile) throws FileNotFoundException, IOException {
		File file = new File(originalFile);
		return setFromFileSystem(file);
	}

	/**
	 * Tries to set the DBDyteArray to the contents of the supplied file.
	 *
	 * <p>
	 * Convenience method for {@link #setFromFileSystem(java.io.File) }.
	 *
	 * @param originalFile	originalFile
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the byte[] of the contents of the file.
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 *
	 *
	 */
	public byte[] setFromFileSystem(DBString originalFile) throws FileNotFoundException, IOException {
		File file = new File(originalFile.stringValue());
		return setFromFileSystem(file);
	}

	/**
	 * Tries to set the DBDyteArray to the contents of the supplied file.
	 *
	 * @param originalFile	originalFile
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the byte[] of the contents of the file.
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 *
	 *
	 */
	public byte[] setFromFileSystem(File originalFile) throws FileNotFoundException, IOException {
		byte[] bytes = new byte[(int) originalFile.length()];
		InputStream input = null;
		try {
			int totalBytesRead = 0;
			input = new BufferedInputStream(new FileInputStream(originalFile));
			while (totalBytesRead < bytes.length) {
				int bytesRemaining = bytes.length - totalBytesRead;
				//input.read() returns -1, 0, or more :
				int bytesRead = input.read(bytes, totalBytesRead, bytesRemaining);
				if (bytesRead > 0) {
					totalBytesRead += bytesRead;
				}
			}
		} finally {
			if (input != null) {
				input.close();
			}
		}
		setValue(bytes);
		return bytes;
	}

	/**
	 * Tries to write the contents of this DBByteArray to the file supplied.
	 *
	 * <p>
	 * Convenience method for {@link #writeToFileSystem(java.io.File) }.
	 *
	 * @param originalFile originalFile
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 */
	public void writeToFileSystem(String originalFile) throws FileNotFoundException, IOException {
		File file = new File(originalFile);
		writeToFileSystem(file);
	}

	/**
	 * Tries to write the contents of this DBByteArray to the file supplied.
	 *
	 * <p>
	 * Convenience method for {@link #writeToFileSystem(java.io.File) }.
	 *
	 * @param originalFile originalFile
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 */
	public void writeToFileSystem(DBString originalFile) throws FileNotFoundException, IOException {
		writeToFileSystem(originalFile.toString());
	}

	/**
	 * Tries to write the contents of this DBByteArray to the file supplied.
	 *
	 * <p>
	 * Convenience method for {@link #writeToFileSystem(java.io.File) }.
	 *
	 * @param originalFile originalFile
	 * @throws java.io.FileNotFoundException java.io.FileNotFoundException
	 * @throws java.io.IOException java.io.IOException
	 */
	public void writeToFileSystem(File originalFile) throws FileNotFoundException, IOException {
		boolean createdNewFile = false;
		if (getLiteralValue() != null && originalFile != null) {
			if (!originalFile.exists()) {
				createdNewFile = originalFile.createNewFile();
			}
			if (originalFile.exists()) {
				OutputStream output = null;
				try {
					output = new BufferedOutputStream(new FileOutputStream(originalFile));
					output.write(getBytes());
					output.flush();
					output.close();
					output = null;
				} finally {
					if (output != null) {
						output.close();
					}
				}
			} else {
				throw new FileNotFoundException("Unable Create File: the file \"" + originalFile.getAbsolutePath() + " could not be found or created.");
			}
		}
	}

	/**
	 * Returns the internal InputStream.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return an InputStream to read the bytes.
	 */
	@Override
	public InputStream getInputStream() {
		if (byteStream == null) {
			this.setValue(getBytes());
		}
		return byteStream;
	}

	/**
	 * Returns the byte[] used internally to store the value of this DBByteArray.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the byte[] value of this DBByteArray.
	 */
	public byte[] getBytes() {
		final Object value = getLiteralValue();
		if (value instanceof InputStream) {
			return getBytesFromInputStream((InputStream) value);
		} else {
			return (byte[]) value;
		}
	}

	@Override
	public String stringValue() {
		byte[] value = this.getValue();
		if (this.isNull()) {
			return super.stringValue();
		} else {
			return new String(value);
		}
	}

	@Override
	public int getSize() {
		final byte[] bytes = getBytes();
		if (bytes != null) {
			return bytes.length;
		} else {
			return 0;
		}
	}

	@Override
	public byte[] getValue() {
		return getBytes();
	}

	@Override
	public DBByteArray getQueryableDatatypeForExpressionValue() {
		return new DBByteArray();
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return new HashSet<DBRow>();
	}

	@Override
	protected Object getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException {
		byte[] bytes = new byte[]{};
		DBDefinition defn = database.getDefinition();
		if (defn.prefersLargeObjectsReadAsBase64CharacterStream()) {
			try {
				bytes = getFromCharacterReader(resultSet, fullColumnName);
			} catch (IOException ex) {
				throw new DBRuntimeException("Unable To Set Value: " + ex.getMessage(), ex);
			}
		} else if (defn.prefersLargeObjectsReadAsBytes()) {
			bytes = getFromGetBytes(resultSet, fullColumnName);
		} else if (defn.prefersLargeObjectsReadAsCLOB()) {
			bytes = getFromCLOB(resultSet, fullColumnName);
		} else if (defn.prefersLargeObjectsReadAsBLOB()) {
			bytes = getFromBLOB(resultSet, fullColumnName);
		} else {
			bytes = getFromBinaryStream(resultSet, fullColumnName);
		}
		return bytes;
	}

	@Override
	public boolean getIncludesNull() {
		return false;
	}

}
