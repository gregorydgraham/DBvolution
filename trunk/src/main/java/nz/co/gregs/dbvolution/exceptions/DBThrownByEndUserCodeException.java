package nz.co.gregs.dbvolution.exceptions;

/**
 * Thrown when the end-user's code threw an exception during invocation. For
 * example, this exception will be thrown when property accessor methods throw
 * exceptions.
 *
 * @author Malcolm Lett
 */
public class DBThrownByEndUserCodeException extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when the end-user's code threw an exception during invocation. For
	 * example, this exception will be thrown when property accessor methods
	 * throw exceptions.
	 *
	 */
	public DBThrownByEndUserCodeException() {
	}

	/**
	 * Thrown when the end-user's code threw an exception during invocation. For
	 * example, this exception will be thrown when property accessor methods
	 * throw exceptions.
	 *
	 * @param message	 message	
	 */
	public DBThrownByEndUserCodeException(String message) {
		super(message);
	}

	/**
	 * Thrown when the end-user's code threw an exception during invocation. For
	 * example, this exception will be thrown when property accessor methods
	 * throw exceptions.
	 *
	 * @param cause	 cause	
	 */
	public DBThrownByEndUserCodeException(Throwable cause) {
		super("An exception was thrown by user code: " + cause.toString(), cause);
	}

	/**
	 * Thrown when the end-user's code threw an exception during invocation. For
	 * example, this exception will be thrown when property accessor methods
	 * throw exceptions.
	 *
	 * @param message message
	 * @param cause cause
	 */
	public DBThrownByEndUserCodeException(String message, Throwable cause) {
		super(message, cause);
	}
}
