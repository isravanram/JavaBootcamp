import java.sql.*;  // For using Connection, Statement and ResultSet
import java.io.*;   // For using File,FileInputStream and FileOutputStream
import java.util.*;
public class jdbcTest {
	public static int size;
	public static int row_size;
	static List<String> id=new ArrayList<String>();
	static List<String> firstNames=new ArrayList<String>();
	static List<String> lastNames=new ArrayList<String>();
	static List<String> dep_id=new ArrayList<String>();
	static List<String> joinDate=new ArrayList<String>();
	static List<String> dob=new ArrayList<String>();
	static List<String> phone=new ArrayList<String>();
	static List<String> email=new ArrayList<String>();
	static List<String> list=new ArrayList<String>();
	static FileOutputStream fout=null;
	
	public static String formatDate(String birthday)
	{
		int seperation1=birthday.indexOf("/");
		int seperation2=birthday.indexOf("/",seperation1+1);
		String day=birthday.substring(0,seperation1);
		String month=birthday.substring(seperation1+1,seperation2);
		String year=birthday.substring(seperation2+1,birthday.length());
		String formatted_date=year+"-"+month+"-"+day;
		return formatted_date;
	}
	
	private static void writeFile(FileOutputStream fout, ResultSet myRs) throws SQLException {
		String id1,fname,lname,depid,doj,birthday,mobile,mail;
		while(myRs.next())
		{	
			id1=myRs.getString("studentId");
			fname=myRs.getString("fullName");
			lname=myRs.getString("lastName");
			depid=myRs.getString("departmentId");
			doj=myRs.getString("joiningDate");
			birthday=myRs.getString("studentDob");
			mobile=myRs.getString("mobileNo");
			mail=myRs.getString("email");
			String text=id1+" 	"+fname+" 	"+lname+" 	"+depid+"	 "+doj+" 	"+birthday+" 	"+mobile+" 	"+mail+"	 "+"\n";
			byte[] bytes=text.getBytes();
			
			try {
				fout.write(bytes);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static void DepartmentFile(FileOutputStream fout, ResultSet myRs) throws SQLException {
		String id1,fname,lname,depid,doj,birthday,mobile,mail,department_name;
		while(myRs.next())
		{	
			id1=myRs.getString("studentId");
			fname=myRs.getString("fullName");
			lname=myRs.getString("lastName");
			doj=myRs.getString("joiningDate");
			birthday=myRs.getString("studentDob");
			mobile=myRs.getString("mobileNo");
			mail=myRs.getString("email");
			department_name=myRs.getString("departmentName");
			String text=id1+" 	"+fname+" 	"+lname+"	 "+doj+" 	"+birthday+" 	"+mobile+" 	"+mail+"	 "+department_name+"\n";
			byte[] bytes=text.getBytes();
			
			try {
				fout.write(bytes);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static void DepartmentCategory(FileOutputStream fout, ResultSet myRs) throws SQLException {
		String total,department_name;
		while(myRs.next())
		{	
			
			department_name=myRs.getString("departmentName");
			total=myRs.getString("total");
			String text=department_name+"	"+total+" "+"\n";
			byte[] bytes=text.getBytes();
			
			try {
				fout.write(bytes);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
	public static void main(String[] args)throws SQLException{
		
		// Reading the file
		File myFile=new File("C:\\Users\\sravan.booraga\\eclipse-workspace\\jdbc-test\\src\\input.txt");
		//Setting file input and output stream variables
		FileInputStream fis=null;
		// perform IO operation
		List<String> rows=new ArrayList<String>();
		
		try
		{
			fis=new FileInputStream(myFile);
			//fout=new FileOutputStream("C:\\Users\\sravan.booraga\\eclipse-workspace\\Getting-started\\src\\intro\\file2.txt");
			int content;
			int counter=0;
			char row;
			String string="";
			int col_count=0;
			row_size=0;
			while((content=fis.read()) !=-1)
			{
				counter+=1;
				row=(char)content;
				if(Character.isWhitespace(row))
				{
					if(col_count==0)
					{
						id.add(string);
					}
					else if(col_count==1)
					{
						firstNames.add(string);
					}	
					else if(col_count==2)
					{
						lastNames.add(string);
					}
					else if(col_count==3)
					{
						dep_id.add(string);
					}	
					else if(col_count==4)
					{
						joinDate.add(string);
					}
					else if(col_count==5)
					{
						dob.add(string);
					}
					else if(col_count==6)
					{
						phone.add(string);
					}	
					else if(col_count==7)
					{
						email.add(string);
					}
					//insert 
					col_count+=1;
					if(col_count == 9)
					{
						col_count=0;
						row_size+=1;
					}
					
					string="";
				}
				else {
					string+=row;
				}				
			}
			System.out.println("------------------------------");
			
			// File containing id to be removed
			File idFile=new File("C:\\Users\\sravan.booraga\\eclipse-workspace\\jdbc-test\\src\\deletestudent.txt");
			FileInputStream in=new FileInputStream(idFile);
			Scanner sc=new Scanner(in);
			int contents;
			int counter2=0;
			char value;
			String str="";
			while(sc.hasNextLine())  
			{  
				list.add(sc.nextLine());
			}  
			sc.close();
			System.out.println("File Copied Successfully");
		}
		catch(IOException e)
		{
			System.out.println(e);
		}
		
		// Setting up database connection variables
		Connection myConn=null;
		Statement myStmt=null;
		ResultSet myRs=null;
		
	try {
		myConn=DriverManager.getConnection("jdbc:mysql://localhost:3306/demo","student","student");
		System.out.println("Database connection successfull");
		myStmt=myConn.createStatement();
		myRs=myStmt.executeQuery("select * from student");
		while(myRs.next())
		{
//			System.out.println(myRs.getString("fullName"));
		}
		int i;
		String id1,fname,lname,mail,mobile,depid,birthday,doj;
		for(i=0;i<row_size;i++)
		{
			id1=id.get(i).toString();
			fname=firstNames.get(i).toString();
			lname=lastNames.get(i).toString();
			depid=dep_id.get(i).toString();
			birthday=dob.get(i).toString();
			doj=joinDate.get(i).toString();
			mobile=phone.get(i).toString();
			mail=email.get(i).toString();
			
			String bday=formatDate(birthday);
			String dateJoin=formatDate(doj);
			// Convert date into correct format
			
			myRs=myStmt.executeQuery("select * from student where studentId="+id1);
			if(myRs.next())
			{	// Code to update the table
				PreparedStatement stmt=myConn.prepareStatement("update student set fullName=?,lastName=?,departmentId=?,joiningDate=?,studentDob=?,mobileNo=?,email=? where studentId=?");  
				stmt.setString(1,fname); 
				stmt.setString(2,lname); 
				stmt.setString(3,depid);
				stmt.setString(4,dateJoin);
				stmt.setString(5,bday);
				stmt.setString(6,mobile);
				stmt.setString(7,mail);
				stmt.setString(8,id1);
				int status=stmt.executeUpdate();  
			}
			else
			{
				// Code to insert the row into the table
//				int rowsAffected=myStmt.executeUpdate("insert into student values);
			           	String query = " insert into student (studentId, fullName, lastName, departmentId, joiningDate, studentDob, mobileNo, email)"
					        + " values (?, ?, ?, ?, ?, ?, ?, ?)";

						  PreparedStatement preparedStmt = myConn.prepareStatement(query);
					      preparedStmt.setString(1,id1);
					      preparedStmt.setString(2,fname);
					      preparedStmt.setString(3,lname);
					      preparedStmt.setString(4,depid);
					      preparedStmt.setString(5,bday);
					      preparedStmt.setString(6,dateJoin);
					      preparedStmt.setString(7,mobile);
					      preparedStmt.setString(8,mail);
					      preparedStmt.execute();
			
			}
		}
		
		// Deleting entries with given ID's
		String id_to_be_deleted;
		int ind;
		for(ind=0;ind<list.size();ind++)
		{
			id_to_be_deleted=list.get(ind);
			int rowsAffected=myStmt.executeUpdate("delete from student where studentId ="+id_to_be_deleted);
		}
	
		// Upload the database content to a file
		
		myRs=myStmt.executeQuery("SELECT * FROM demo.student where departmentId in (SELECT departmentId FROM demo.department where departmentName=\"EEE\")");
		fout=new FileOutputStream("C:\\Users\\sravan.booraga\\eclipse-workspace\\jdbc-test\\src\\EEE_data.txt");
		writeFile(fout,myRs);
		
		myRs=myStmt.executeQuery("SELECT studentId,fullName,lastName,joiningDate,studentDob,mobileNo,email,departmentName FROM demo.student JOIN demo.department where demo.student.departmentId=demo.department.departmentId;");
		fout=new FileOutputStream("C:\\Users\\sravan.booraga\\eclipse-workspace\\jdbc-test\\src\\department-student-data.txt");
		DepartmentFile(fout,myRs);
		
		myRs=myStmt.executeQuery("SELECT COUNT(demo.student.departmentId) as total,departmentName from demo.student JOIN demo.department where demo.department.departmentId=demo.student.departmentId group by departmentName");
		fout=new FileOutputStream("C:\\Users\\sravan.booraga\\eclipse-workspace\\jdbc-test\\src\\department-groups.txt");
		DepartmentCategory(fout,myRs);
	}
	
	catch (Exception exc)
	{
		System.out.println(exc);
	}
	}
}
