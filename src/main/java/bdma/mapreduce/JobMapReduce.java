package bdma.mapreduce;

import java.io.IOException;

public abstract class JobMapReduce {

	protected String input;
	protected String output;
	
	public void setInput(String input) {
		this.input = input;
	}
	
	public void setOutput(String output) {
		this.output = output;
	}
	
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		return false;
	}

}
