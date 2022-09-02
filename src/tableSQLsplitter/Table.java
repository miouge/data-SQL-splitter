package tableSQLsplitter;

import java.util.ArrayList;

public class Table {

	public String name;
	
	// declaration
	public ArrayList<String> declaration = new ArrayList<>();

	// indexes
	public ArrayList<String> indexes = new ArrayList<>();	
	
	// constraints
	public ArrayList<String> constraints = new ArrayList<>();
	
	//dependencie
	public ArrayList<Table> dependancies = new ArrayList<>();	
	
	// wrote on disk
	public boolean written = false;
}
