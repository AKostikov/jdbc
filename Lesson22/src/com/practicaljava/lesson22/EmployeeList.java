package com.practicaljava.lesson22;

import java.sql.*;

class EmployeeList {
	
  public static void outputApartments(ResultSet rs, int i,  PreparedStatement prepstmt) throws SQLException{
	  	prepstmt.setInt(1, i);
	  	rs = prepstmt.executeQuery();	
		while (rs.next()){ 
		    	 int number = rs.getInt("Number");
		       	 int buildingID = rs.getInt("BuildingID");
			     System.out.println(""+ number + ", " + buildingID); 
		}
  }
  
  public static void RollBack(Connection conn){
	  try{
		  if(conn.getAutoCommit())
			  conn.rollback();
	  }
	  catch( SQLException se ) {
	      System.out.println ("SQLError: " + se.getMessage ()
	           + " code: " + se.getErrorCode ());

	   }
  }
  

  public static void main(String argv[]) {
   Connection conn=null;
   Statement stmt=null;
   PreparedStatement prepstmt=null;
   ResultSet rs=null;
   

   try {
     Class.forName("org.apache.derby.jdbc.ClientDriver");
     conn = DriverManager.getConnection("jdbc:derby:RealtyDB;create=true");
     String sqlCreateBuildingsTable = "CREATE TABLE Buildings(BuildingID int primary key generated always as identity," +
     		" Street varchar (50), Number int )";
     String sqlCreateApartmentsTable = "CREATE TABLE Apartments(ApartmentID int primary key generated always as identity," +
     		" Number int, BuildingID int references Buildings(BuildingID))";
     String sqlInsertBuildings = "INSERT INTO Buildings (Street, Number ) VALUES ('Бауманская', 11), ('Стромынка', 22)";
     String sqlInsertApartments = "INSERT INTO Apartments (Number, BuildingID) VALUES (141, 1), (142, 1), (95, 2), (96, 2)";
     String sqlSimpleSelect = "Select * from Buildings";
     String sqlParamSelect = "Select * from Apartments where BuildingID=?";
     
     stmt = conn.createStatement();
     
     conn.setAutoCommit(false);
     
     stmt.addBatch(sqlCreateBuildingsTable);
     stmt.addBatch(sqlCreateApartmentsTable);
     stmt.addBatch(sqlInsertBuildings);
     stmt.addBatch(sqlInsertApartments);
     
     stmt.executeBatch();
     conn.commit();
          
    
    conn.setAutoCommit(true);
     
    prepstmt = conn.prepareStatement(sqlParamSelect);
     
    rs = stmt.executeQuery(sqlSimpleSelect);  
    
    System.out.println("Обычное выполнение запроса: ");
    while (rs.next()){ 
    	 String street = rs.getString("Street");
       	 int number = rs.getInt("Number");
	     System.out.println(""+ street + ", " + number); 
    }
    
    System.out.println("Прекомпилированный запрос: ");
    System.out.println("Первый дом: ");
    outputApartments(rs, 1, prepstmt);
    System.out.println("Второй дом: ");
    outputApartments(rs, 2, prepstmt);

   } catch( SQLException se ) {
	  RollBack(conn);
      System.out.println ("SQLError: " + se.getMessage ()
           + " code: " + se.getErrorCode ());

   } catch( Exception e ) {
      System.out.println(e.getMessage()); 
      e.printStackTrace(); 
   } finally{
       try{
	   rs.close();     
	   stmt.close(); 
	   conn.close();  
       } catch(Exception e){
           e.printStackTrace();
       } 
   }
}
}
