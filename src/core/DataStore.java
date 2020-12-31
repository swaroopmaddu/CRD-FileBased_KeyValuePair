package core;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.DosFileAttributes;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataStore {
	private File database;
	private String DATA_SEPARATOR  = "::";
	private String filePath;
	//DataStore constructor
	public DataStore(String path) throws DatabaseExceptions, IOException {
		filePath = path;
		database = new File(path); 
		//if file was already in use 
		if(isLocked(filePath)) {
			throw new DatabaseExceptions("File already in use.");
		}
		
		//if file not exits
        if (!database.exists()) {
        	try {
        		//try to crate new file
				if (database.createNewFile()) {
				    System.out.println("File created: " + database.getName());
				  } else {
					  System.out.println("An error occurred while creating database file.");
				  }
			} catch (IOException e) {
				System.out.println("An error occurred while creating database file.");
				e.printStackTrace();
			}
        }
        
        if(isValidSize()) {
            lockFile();
        } else {
        	throw new DatabaseExceptions("File size limit exeeded.");
        }
        
    }
	

	//DataStore constructor if no file provided
	public DataStore() throws DatabaseExceptions, IOException {
		String fileName = new SimpleDateFormat("yyyyMMddHHmm'.txt'").format(new Date());
		filePath = System.getProperty("user.home")+"\\DataStore";
	    
		System.out.println(filePath);   
    	//try to create new folder if not exits
		if(!isDirectoryExists(filePath)) {
			  if (new File(filePath).mkdirs()) {	
				  System.out.println("New folder created for databases.");
			  } else {
				  System.out.println("An error occurred while creating newww database file.");
			  }
		}
		filePath = filePath+"\\"+fileName;
		database = new File(filePath);
		if (!database.exists()) {
			database.createNewFile();
		} 
	    System.out.println("File created: " + database.getName());
	    lockFile();
    }
	
	private void lockFile() {
		try {
    		//try to create new file
			if (new File(filePath+".lock").createNewFile()) {
			    
			  } else {
				  System.out.println("An error occurred while locking database file.");
				  System.exit(0);
			  }
		} catch (IOException e) {
			System.out.println("An error occurred while locking database file.");
			e.printStackTrace();
		}
	}
	//get file size in MB
	private boolean isValidSize() {
		return ((double) this.database.length() / (1024 * 1024)) < 1024 ;
	}
	//check whether the file is locked or not
	private boolean isLocked(String path) {
		return new File(path+".lock").exists();
	}
	//Remove lock of a file
	private void removeLock() throws IOException {
		if (new File(filePath+".lock").delete()) {
	        
	    } else {
	        throw new IOException("Could not delete lock file.");
	    }
	}
	//Check weather the directory for DataStore exists or not
	private static boolean isDirectoryExists(String directoryPath)  {
	    if (!Paths.get(directoryPath).toFile().isDirectory())
	    {
	      return false;
	    }
	    return true;
	  }

	//Insert key into database
	private void insert(String key,String value,int ttl) {
		String inputFileName = filePath;
		//Create a temporary file
		String outputFileName = filePath+".temp";
		
		try {
		    File inputFile = new File(inputFileName);
		    synchronized(inputFile) {
		    	  // Perform file manipulations
			    File outputFile = new File(outputFileName);
			    synchronized(outputFile) {
			    try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
			        // Read each line from the reader and compare it with key to remove
			        String line = null;
			        boolean isAdded = false;
			        while ((line = reader.readLine()) != null) {
			            if (getKey(line).compareToIgnoreCase(key)>0 && !isAdded) {
			            	if(ttl<0)
			            		writer.write(key+DATA_SEPARATOR+value+System.lineSeparator());
			            	else
					        	writer.write(key+DATA_SEPARATOR+value+DATA_SEPARATOR+new Date(System.currentTimeMillis()+ttl*100)+System.lineSeparator());
			            	isAdded = true;
			            } else if(getKey(line).compareToIgnoreCase(key)==0) {
			            	System.out.println("Key is already defined.");
			            	isAdded = true;
			            	continue;
			            }
		            	writer.write(line);
		            	writer.newLine();
			        }
			        //Add at end of file
			        if(!isAdded) {
			        	writer.write(key+DATA_SEPARATOR+value+System.lineSeparator());
		            	isAdded = true;
			        }
			        reader.close();
			        writer.close();
			    }
			    // Delete the original file
			    if (inputFile.delete()) {
			        // Rename the output file to the input file
			        if (!outputFile.renameTo(inputFile)) {
			            throw new IOException("Could not rename " + outputFileName + " to " + inputFileName);
			        }
			    } else {
			        throw new IOException("Could not delete original input file " + inputFileName);
			    }
			    }
			  }
			} catch (IOException ex) {
			    // Handle other exceptions
			    ex.printStackTrace();
			}
	}
	
	//Method to insert or create new Key Value pair
	public void create(String key,String value) throws Exception {
		if(isConsistent(key,value)) {				
			insert(key,value,-1);
		}
	}
	
	//print file stats
	public void stats() throws IOException {
		Path path = Paths.get(filePath);
		DosFileAttributes attr = Files.readAttributes(path, DosFileAttributes.class);
		System.out.println("Created at   = " + attr.creationTime());
        System.out.println("LastModified = " + attr.lastAccessTime());
        System.out.println("LastModified = " + attr.lastModifiedTime());
        System.out.println("File Size    = " + attr.size());
        System.out.println("----------------------------------------");
	}

	private boolean isConsistent(String key,String value) throws Exception {
		//constraint Key size
		if(key.length()>32) {
			throw new Exception("Key should be maximum 32 chars.");
		}
		//constraint Value size
		if(value.length()>16000 ) {
			throw new Exception("Value should be maximum 16000 chars(16KB).");
		}
		//check file size
		if(!isValidSize()) {
        	throw new DatabaseExceptions("File size limit exeeded.");
        }
		return true;
	}
	//Method to insert or create new Key Value pair with Time to live
	public void create(String key,String value,int ttl) throws Exception {
		if(isConsistent(key,value)) {				
			insert(key,value,ttl);
		}
	}
	private String getKey(String text) {
		return text.substring(0, text.indexOf(DATA_SEPARATOR));
	}
	//to check weather the object is expired or not
	private boolean isAlive(String strDate) throws ParseException {
		DateFormat formatter = new SimpleDateFormat("EEE MMM d HH:mm:ss z yyyy");
		Date end =  formatter.parse(strDate);
		Date now = new Date(System.currentTimeMillis());
		return end.compareTo(now)==1;
	}
	//get value of a key
	public String read(String key) throws IOException, ParseException {
		 String line;
		 BufferedReader br = new BufferedReader(new FileReader(filePath));
		 while((line = br.readLine()) != null) {
	            if(getKey(line).equals(key)) {
	            	String arr[] = line.split(DATA_SEPARATOR);
	            	if(arr.length>2) {
	            		if(isAlive(arr[2])) {
	            			br.close();
	            			return arr[1];
	            		}else {
	            			br.close();
	            			delete(key); //if TTL is expired delete key-Value pair
		            		break;
	            		}
	            	}
        			br.close();
	            	return arr[1];
	            }
	        }
		br.close();
		return "Key not found"; //If key was not found
	}

	
	public void delete(String key) { 
		String inputFileName = filePath;
		String outputFileName = filePath+".temp";
		try {
		    File inputFile = new File(inputFileName);
		    synchronized(inputFile) {
		    File outputFile = new File(outputFileName);
		    synchronized(outputFile) {
		    try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
		             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
		        // Read each line from the reader and compare it with key to remove
		        String line = null;
		        //write all KeyValue pairs except given key
		        while ((line = reader.readLine()) != null) {
		            if (!getKey(line).equals(key)) {
		                writer.write(line);
		                writer.newLine();
		            }
		        }
		        reader.close();
		        writer.close();
		    	}
		    }
		    // Delete the original file
		    if (inputFile.delete()) {
		        // Rename the output file to the input file
		        if (!outputFile.renameTo(inputFile)) {
		            throw new IOException("Could not rename " + outputFileName + " to " + inputFileName);
		        }
		    } else {
		        throw new IOException("Could not delete original input file " + inputFileName);
		    }
		    }
		} catch (IOException ex) {
		    // Handle other exceptions
		    ex.printStackTrace();
		}
	}
	
	//close and remove database connection lock
	public void close() throws IOException {
		removeLock();
	}
}
