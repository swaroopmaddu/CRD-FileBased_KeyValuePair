package core;

@SuppressWarnings("serial")
public class DatabaseExceptions extends Exception{
	private String exception;
	public DatabaseExceptions(String ex) {
		exception = ex;
	}
	public String toString() {
		return ("Exception : "+exception);
	}
}
