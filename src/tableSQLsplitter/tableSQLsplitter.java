package tableSQLsplitter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.TreeMap;

public class tableSQLsplitter {

	static void parseTable( TreeMap< String, Table > tables ) throws FileNotFoundException, IOException {
		
		String tableSQLPath = "D:/GIT/SRV65EE/SRV/BDD-PG/ora2pg/schema/tables/table.sql";
	
		Table table = null;
		Table previous = null;
		
		int lineNum = 0;
		try( BufferedReader reader = new BufferedReader( new FileReader( tableSQLPath )) )
		{
			while( true ) {

				String line = reader.readLine();
				if( line == null ) { break; }

				lineNum++;

				String words[] = line.split(" ");		

				// CREATE TABLE adc (
				if( table == null && words[0].equals("CREATE") && words[1].equals("TABLE") ) {
					
					table = new Table();
					previous = table;
					table.name = words[2].replace("\"", "");
					
					// System.out.format("create table %s ...", table.name );
					tables.put( table.name, table );
					table.declaration.add( line );
					table.declaration.add( "" );
				}
				// ) ;
				else if( table != null && words[0].equals(")") && words[1].equals(";") ) {
					
					// System.out.format(" OK\n" );
					table.declaration.add( "" );
					table.declaration.add( ");" );
					table = null;
				}
				else if( table != null ) {
					
					/*
					if( words[0].equals("\ttimestamp")) {
						
						// commented this fields
						table.declaration.add( "-- " + line );
					}
					else {

						table.declaration.add( line );
					}
					*/
					
					table.declaration.add( line );
				}
				else if( table == null && words[0].equals("CREATE") && previous != null ) {
				
					previous.indexes.add( line );
				}
				else {
					
					if( line.length() > 0 ) {
						System.out.format("table.sql : ignored line %d> %s\n", lineNum, line );
					}
				}
			}
		}
	}
	
	static void parseConstraints( TreeMap< String, Table > tables ) throws FileNotFoundException, IOException {
		
		String tableSQLPath = "D:/GIT/SRV65EE/SRV/BDD-PG/ora2pg/schema/tables/CONSTRAINTS_table.sql";
	
		int fkNb = 0;
		int lineNum = 0;
		try( BufferedReader reader = new BufferedReader( new FileReader( tableSQLPath )) )
		{
			while( true ) {

				String line = reader.readLine();
				if( line == null ) { break; }
				
				lineNum++;
			
				String words[] = line.split(" ");		
														
				// ALTER TABLE aqi_model ADD UNIQUE (label);
				// ALTER TABLE mempart ADD CONSTRAINT fk_mempart_ressurveil FOREIGN KEY (nressurv) REFERENCES res_surveil(nressurv) ON DELETE NO ACTION NOT DEFERRABLE INITIALLY IMMEDIATE;
				//  0     1      2      3     4               5                6     7      8          9           10				
				
				if( words.length >= 3 && words[0].equals("ALTER") && words[1].equals("TABLE") ) {
					
					String tablename = words[2].replace("\"", "");
					Table tableSRC = tables.get( tablename );
					
					if( words.length >= 10 && words[6].equals("FOREIGN") && words[9].equals("REFERENCES") ) {
						
						String[] refs = words[10].split( "\\(" );
						Table tableREF = tables.get( refs[0] );
						tableSRC.dependancies.add( tableREF );
						tableSRC.constraints.add( line );

						// System.out.format("FK : %s -> %s\n",  tableSRC.name, tableREF.name );
						fkNb++;						
					}
					else {
						
						if( line.length() > 0 ) {
							tableSRC.constraints.add( line );
						}
					}
				}
				else {
					
					if( line.length() > 0 ) {
						System.out.format("CONSTRAINTS_table.sql : ignored line %d> %s\n", lineNum, line );
					}
				}
			}
		}
		
		System.out.format("FK count %d\n", fkNb );
	}	
	
	static void output( TreeMap< String, Table > tables ) throws IOException {
			
		
		String fileM   = "D:/GIT/SRV65EE/SRV/BDD-PG/create/schema/create-tables.sql";
		BufferedWriter writerM = new BufferedWriter(new FileWriter(fileM));		
				
		writerM.write( "\n" );
		writerM.write( "SET client_encoding TO 'UTF8';\n" );
		writerM.write( "\\set ON_ERROR_STOP ON\n" );
		writerM.write( "\n" );
		writerM.write( "CREATE SCHEMA rsdba;\n" );
		writerM.write( "ALTER SCHEMA rsdba OWNER TO rsdba;\n" );
		writerM.write( "\n" );
		writerM.write( "SET search_path = rsdba;\n" );
		writerM.write( "\n" );		

		do {

			int writtenNb = 0;

			for( String tablename : tables.keySet() ) {

				Table table = tables.get( tablename );

				if( table.written == true ) {
					writtenNb++;
				}
			}

			System.out.format("table written %d\n", writtenNb );

			if( writtenNb == tables.keySet().size() ) {

				// all written finished
				break;
			}

			for( String tablename : tables.keySet() ) {

				Table table = tables.get( tablename );
				
				if( table.written == true ) {
					continue;
				}
				
				// could we write the table ?
				
				boolean writeAble = true;
								
				for( Table dependacy : table.dependancies ) {

					if( dependacy.written != true ) {
						writeAble = false;
						break;
					}
				}
				
				if( writeAble == false ) { 
					continue;
				}
								
				// System.out.format("write table %s\n", table.name );
				
				writerM.write( "\\i ./schema/s_tables/" + table.name + ".sql\n" );
				
				String fileT = "D:/GIT/SRV65EE/SRV/BDD-PG/create/schema/s_tables/" + table.name + ".sql";
				BufferedWriter writerT = new BufferedWriter(new FileWriter(fileT));

				writerT.write( "\n" );
				writerT.write( "SET client_encoding TO 'UTF8';\n" );
				writerT.write( "\\set ON_ERROR_STOP ON\n" );
				writerT.write( "SET search_path = rsdba;\n" );
				writerT.write( "\n" );
				
				for( String line : table.declaration ) {
					
					writerT.write( line + "\n" );
				}
				
				if(  table.indexes.size() > 0 ) {
					
					writerT.write( "\n" );
					for( String line : table.indexes ) {
						
						writerT.write( line + "\n" );
					}								
				}				

				if(  table.constraints.size() > 0 ) {
					writerT.write( "\n" );
					for( String line : table.constraints ) {
						
						writerT.write( line + "\n" );
					}					
				}
								
				writerT.close();
				table.written = true;
			}
			
		}
		while( true );		
		
		writerM.close();
	}
	
	public static void main(String[] args) {
		
		TreeMap< String, Table > tables = new TreeMap<>();
		
		try {
			
			parseTable( tables );
			parseConstraints( tables );
			output( tables );
			System.out.format("done\n" );
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
