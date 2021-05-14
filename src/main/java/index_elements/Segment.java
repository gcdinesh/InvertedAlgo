package index_elements;

public class Segment {
	private int threadNumber;
	private String string;
	private int lineNumber;
	
	public Segment(int threadNumber,String string, int lineNumber) {
		this.threadNumber = threadNumber;
		this.string = string;
		this.lineNumber = lineNumber;
	}

	public int getThreadNumber() {
		return threadNumber;
	}

	public void setThreadNumber(int threadNumber) {
		this.threadNumber = threadNumber;
	}

	public String getString() {
		return string;
	}

	public void setString(String string) {
		this.string = string;
	}

	public int getLineNumber() {
		return lineNumber;
	}

	public void setLineNumber(int lineNumber) {
		this.lineNumber = lineNumber;
	}
}