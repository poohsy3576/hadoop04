package com.example.mysql;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DbToHdfsSender {

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		String cls = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://localhost:3306/world";
//		String url = "jdbc:mysql://ec2:3306/world";
		String user = "jspbook";
		String password = "1234";
		
		Class.forName(cls);
		
		Connection con = DriverManager.getConnection(url, user, password);
		
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		//HDFS에 저장하는 역할
		sendCity(con, hdfs);
		sendCountry(con, hdfs);
		sendCountryLanguage(con, hdfs);
		
		con.close();
		System.out.println("End...");
		
	}
	
	static void sendCity(Connection con, FileSystem hdfs) throws SQLException, IOException {
		PreparedStatement pstmt = con.prepareStatement("select * from city order by id");
		ResultSet rs =  pstmt.executeQuery();
		
		FSDataOutputStream out = hdfs.create(new Path("/home/java/world/city.txt"));
		PrintWriter writer = new PrintWriter(out);
		
		while(rs.next()) {
			//console에 출력
//			System.out.println(rs.getString("id"));
			
			//city.txt 파일에 출력
//			out.writeUTF(rs.getString("id"));
			
			System.out.print(".");
			writer.printf("%10s %-30s %10s %-30s %10s", 	rs.getString("id"), 
												 				rs.getString("name"), 
												 				rs.getString("countryCode"),
												 				rs.getString("district"), 
												 				rs.getString("population"));
			writer.println();
		}
		
		writer.close();
		out.close();
		rs.close();
		pstmt.close();
		
	}

	static void sendCountry(Connection con, FileSystem hdfs) throws SQLException, IOException {
		PreparedStatement pstmt = con.prepareStatement("select * from country");
		ResultSet rs =  pstmt.executeQuery();
		
		FSDataOutputStream out = hdfs.create(new Path("/home/java/world/country.txt"));
		PrintWriter writer = new PrintWriter(out);
		
		while(rs.next()) {
			
			System.out.print(".");
			writer.printf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", 	
																rs.getString("Code"), 
												 				rs.getString("Name"), 
												 				rs.getString("Continent"),
												 				rs.getString("Region"), 
												 				rs.getString("SurfaceArea"),
												 				rs.getString("IndepYear"),
												 				rs.getString("Population"),
												 				rs.getString("LifeExpectancy"),
												 				rs.getString("GNP"),
												 				rs.getString("GNPOld"),
												 				rs.getString("LocalName"),
												 				rs.getString("GovernmentForm"),
												 				rs.getString("HeadOfState"),
												 				rs.getString("Capital"),
												 				rs.getString("Code2"));
			writer.println();
		}
		
		writer.close();
		out.close();
		rs.close();
		pstmt.close();
	}
	
	static void sendCountryLanguage(Connection con, FileSystem hdfs) throws SQLException, IOException {
		PreparedStatement pstmt = con.prepareStatement("select * from countrylanguage");
		ResultSet rs =  pstmt.executeQuery();
		
		FSDataOutputStream out = hdfs.create(new Path("/home/java/world/countrylanguage.txt"));
		PrintWriter writer = new PrintWriter(out);
		
		while(rs.next()) {
			
			System.out.print(".");
			writer.printf("%s,%s,%s,%s", 	rs.getString("countryCode"), 
												 				rs.getString("Language"), 
												 				rs.getString("IsOfficial"),
												 				rs.getString("Percentage"));
			writer.println();
		}
		
		writer.close();
		out.close();
		rs.close();
		pstmt.close();
	}
}
