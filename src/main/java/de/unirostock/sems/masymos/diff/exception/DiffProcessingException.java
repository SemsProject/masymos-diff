package de.unirostock.sems.masymos.diff.exception;

public class DiffProcessingException extends DiffException {

	private static final long serialVersionUID = 7590372612745556904L;

	public DiffProcessingException() {
	}

	public DiffProcessingException(String message, Throwable cause) {
		super(message, cause);
	}

	public DiffProcessingException(String message) {
		super(message);
	}

	public DiffProcessingException(Throwable cause) {
		super(cause);
	}

}
