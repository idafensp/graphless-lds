package oeg.upm.isantana.ldviz.sparql.sql;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.*;

import oeg.upm.isantana.ldviz.sql.util.DataSource;


//https://www.tutorialspoint.com/jdbc/jdbc-create-tables.htm
public class SQLHelper {
	
  
	   private Connection conn;
	   private String graphName;
	   
	   public SQLHelper(String gn)
	   {
		   this.graphName = gn;
		   this.init();
	   }
	   
	   public void init()
	   {
		 //STEP 2: Register JDBC driver
		 try {
			 Class.forName("com.mysql.jdbc.Driver");
			 //STEP 3: Open a connection
			 System.out.println("Connecting to a selected database...");
			 //conn = DriverManager.getConnection(DB_URL, USER, PASS);
			 conn = DataSource.getInstance().getConnection();
			 System.out.println("Connected database successfully...");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PropertyVetoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	   }
	   
	   public void close()
	   {
			try {
				if(conn!=null)
					conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	   }
	   
	   public void createTableRLabels(String tn)
	   {
		   	String tableName = graphName+"_"+tn;
		    String sql = 
		    		  "CREATE TABLE "+tableName+" ( \n" +
		    		  "  id int UNSIGNED NOT NULL AUTO_INCREMENT,\n" +
		    		  "  resource text NOT NULL,\n" +
		    		  "  label text NOT NULL,\n" +
		    		  "  lang text,\n" +
		    		  "  PRIMARY KEY (ID) \n"+
		    		  ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";
		    
		    this.createTable(tableName, sql);
	   }
	   
	   public void createTableRPCountNorm(String tn)
	   {
		   	String tableName = graphName+"_"+tn;
		    String sql = 
		    		  "CREATE TABLE "+tableName+" ( \n" +
		    		  "  id int UNSIGNED NOT NULL AUTO_INCREMENT,\n" +
		    		  "  resource text NOT NULL,\n" +
		    		  "  property text NOT NULL,\n" +
		    		  "  count int(10) UNSIGNED NOT NULL,\n" +
		    		  "  normalized DOUBLE UNSIGNED NOT NULL,\n" +
		    		  "  PRIMARY KEY (ID) \n"+
		    		  ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";
		    
		    this.createTable(tableName, sql);
	   }
	   
	   public void createTableRTPCount(String tn)
	   {
		   	String tableName = graphName+"_"+tn;
		    String sql = 
		    		  "CREATE TABLE "+tableName+" ( \n" +
		    		  "  id int UNSIGNED NOT NULL AUTO_INCREMENT,\n" +
		    		  "  resource text NOT NULL,\n" +
		    		  "  type text NOT NULL,\n" +
		    		  "  property text NOT NULL,\n" +
		    		  "  count int(10) UNSIGNED NOT NULL,\n" +
		    		  "  PRIMARY KEY (ID) \n"+
		    		  ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";
		    
		    this.createTable(tableName, sql);
	   }

	   private void createTable(String tableName, String createSql)
	   {
		   
			System.out.println("About to create table " + tableName);

		   
		   try{
		      
		      //STEP 4: Execute a query
		      System.out.println("Creating table in given database...");
		      Statement stmt= conn.createStatement();
		      
		      String dropSql = "DROP TABLE IF EXISTS "+tableName+";\n";
		      


		      System.out.println("Executing query DROP SQL:");
		      System.out.println(dropSql);
		      stmt.executeUpdate(dropSql);
		      
		      
		      System.out.println("Executing query CREATE SQL:");
		      System.out.println(createSql);
		      stmt.executeUpdate(createSql);
		      
		      
		      System.out.println("Created table in given database...");
		   }catch(SQLException se){
		      //Handle errors for JDBC
		      se.printStackTrace();
		   }catch(Exception e){
		      //Handle errors for Class.forName
		      e.printStackTrace();
		   }
		   System.out.println("Goodbye!");
	   }
	   
	   public void insertRTPCount(String tn, String res, String type, String prop, int count)
	   {

		   String tableName = graphName+"_"+tn;
		   String sql = "INSERT INTO "+tableName+"  (`resource`, `type`, `property`, `count`) " +
		                   "VALUES (\""+res+"\", \""+type+"\", \""+prop+"\", "+count+")";

		  this.insert(sql);
	   }
	   
	   public void insertResLabel(String tn,  String res, String label, String lang)
	   {
		   String tableName = graphName+"_"+tn;
		   String sqlLang = "";
		   if(lang!=null)
			   sqlLang=lang;

		   String sql = "INSERT INTO "+tableName+"  (`resource`, `label`, `lang`) " +
		                   "VALUES (\""+res+"\", \""+label+"\", \""+sqlLang+"\")";
		   
		   insert(sql);
		   
	   }
	   

	   public void insertRPCountNorm(String tn,  String res, String prop, int count, double norm)
	   {
		  String tableName = graphName+"_"+tn;
		   
		  String sql ="INSERT INTO `"+tableName+"` (`resource`, `property`, `count`, `normalized`) "+
				  	" VALUES (\""+res+"\", \""+prop+"\", "+count+", "+norm+")";
		   
		  insert(sql);
		   
	   }
	   

	   private void insert(String insertSql)
	   {

		   try{
		      //STEP 2: Register JDBC driver
		      Class.forName("com.mysql.jdbc.Driver");

		      //STEP 3: Open a connection
		      System.out.println("Connecting to a selected database...");
		      System.out.println("Connected database successfully...");
		      
		      //STEP 4: Execute a query
		      System.out.println("Inserting records into the table...");

			  Statement stmt = conn.createStatement();
		      

		      System.out.println("Inserting sql:");
		      System.out.println(insertSql);
		      stmt.executeUpdate(insertSql);

		      System.out.println("Inserted records into the table...");

		   }catch(SQLException se){
		      //Handle errors for JDBC
		      se.printStackTrace();
		   }catch(Exception e){
		      //Handle errors for Class.forName
		      e.printStackTrace();
		   }
		   System.out.println("Goodbye!");
	   }
	   
	   public ResultSet selectRTPCountsSum(String tn)
	   {
		   	String tableName = graphName+"_"+tn;
		    
		    String sql = "SELECT resource, property, SUM(count) FROM "+tableName+" GROUP BY resource,property";

		    return select(tableName, sql);
	   }
	   
	   public ResultSet selectRPMaxCount(String tn)
	   {
		   String tableName = graphName+"_"+tn;

		   String sql = "SELECT resource, MAX(count) FROM "+tableName+" GROUP BY resource ORDER BY count DESC";
		   
		   return select(tableName, sql);
	   }
	   
	   public ResultSet selectRPNormalized(String tn, String res, String prop)
	   {
		   String tableName = graphName+"_"+tn;

		   String sql = "SELECT normalized FROM "+tableName+" WHERE `resource`=\"" + res +"\" AND `property`=\"" + prop + "\"";
		   
		   return select(tableName, sql);
		   
	   }
	   
	   private ResultSet select(String tablename, String selectSql)
	   {

		   try{
			      //STEP 2: Register JDBC driver
			      Class.forName("com.mysql.jdbc.Driver");

			      //STEP 3: Open a connection
			      System.out.println("Connecting to a selected database...");
			      System.out.println("Connected database successfully...");
			      
			      //STEP 4: Execute a query
			      System.out.println("Creating statement...");
			      Statement stmt = conn.createStatement();


			      System.out.println("Select query:");
			      System.out.println(selectSql);
			      ResultSet rs = stmt.executeQuery(selectSql);
			      
			      return rs;

			   }catch(SQLException se){
			      //Handle errors for JDBC
			      se.printStackTrace();
			   }catch(Exception e){
			      //Handle errors for Class.forName
			      e.printStackTrace();
			   }

		   return null;
	   }
 
	   public void updateMaxNorm(String tn, String res, int max)
	   {
		   
		   String tableName = graphName+"_"+tn;

		   String sql = "UPDATE " + tableName + " SET `normalized` = `count`/" + max + " WHERE `resource`=\"" + res +"\"";
		   		   
		   update(tableName, sql);
		  
	   }
	   
	   private void update(String tablename, String updateSql)
	   {

		   try{
			      //STEP 2: Register JDBC driver
			      Class.forName("com.mysql.jdbc.Driver");

			      //STEP 3: Open a connection
			      System.out.println("Connecting to a selected database...");
			      System.out.println("Connected database successfully...");
			      
			      //STEP 4: Execute a query
			      System.out.println("Creating statement...");
			      Statement stmt = conn.createStatement();


			      System.out.println("Update query:");
			      System.out.println(updateSql);
			      stmt.executeUpdate(updateSql);
			      
			   }catch(SQLException se){
			      //Handle errors for JDBC
			      se.printStackTrace();
			   }catch(Exception e){
			      //Handle errors for Class.forName
			      e.printStackTrace();
			   }

	   }



}
