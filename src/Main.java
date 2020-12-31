import core.DataStore;

public class Main {
	public static void main(String[] args) throws Exception {
		
		DataStore database = new DataStore("C:\\Users\\Sai\\Downloads\\sample.db");
		database.create("Orange", "{color:Orange}");
		database.create("Grapes", "{color:Green}");
		database.create("Berry", "{color:Blue}");
		DataStore database1 = new DataStore("C:\\Users\\Sai\\Downloads\\swaroop.db");
		database.create("Apple", "{color:Red}",2);
		System.out.println(database.read("Apple"));
		database.delete("Berry");
		database.stats();
		Thread.sleep(30);
		System.out.println(database.read("Apple"));
		database.close();
		database1.close(); 
	}

}
