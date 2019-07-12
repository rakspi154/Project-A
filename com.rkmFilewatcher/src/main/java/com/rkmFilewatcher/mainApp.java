package com.rkmFilewatcher;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.sql.Date;
import com.rkmFilewatcher.databaseclass;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.apache.commons.codec.digest.DigestUtils;
//import org.elasticsearch.core.internal.io.IOUtils;
import org.apache.lucene.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.impl.auth.NTLMEngine;
import org.apache.http.impl.auth.*;
//import org.apache.commons.codec.binary.Base64;
import jcifs.smb.*;



import java.util.*;
import java.util.Base64;
import java.lang.String;


public class mainApp {

	private static ArrayList<File> files = new ArrayList<File>();
	public static void main( String[] args )
	{
		try {
			System.out.println( "Job Initiated" );

			//*************************************************
			//*********Retrieve Properties value from config***
			//*************************************************

			Properties objProp = new Properties();
			InputStream input = null;
			input = new FileInputStream("config.properties");
			objProp.load(input);

			Calendar calendar = Calendar.getInstance();
			java.sql.Date startDate = new java.sql.Date(calendar.getTime().getTime());

			Date stDate = 	startDate;
			long timeMilli = startDate.getTime();
			String strdir = objProp.getProperty("path");
			String strElkHost = objProp.getProperty("elastichost");
			int intElkPort = Integer.parseInt(objProp.getProperty("elasticport"));
			int intElkPort1 = Integer.parseInt(objProp.getProperty("elasticport1"));
			String strProto = objProp.getProperty("elasticprotocol");
			String strIndexName = objProp.getProperty("elasticIndex");
			String strPipeline = objProp.getProperty("pipleline");
			String strURL = objProp.getProperty("database");
			String strUserName = objProp.getProperty("dbuser");
			String strPassword = objProp.getProperty("dbpassword");

			//*************************************************
			//*********Read from network path******************
			//*************************************************


			//String user = "user:password";
			//NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(user);
			//String path = objProp.getProperty("path");
			//SmbFile sFile = new SmbFile(path, auth);
			//String strFilepath = sFile.getPath();

			List<File> files = fnfetechListFilesandDirectory(strdir);





			File[] fAllFile = files.toArray(new File[files.size()]);

			if(fAllFile != null && fAllFile.length > 0)
			{

				for (int i=0; i <fAllFile.length; i++)
				{
					String flag = null;



					String strOriginalFile = fAllFile[i].getAbsolutePath();


					if(fAllFile[i].isFile())
					{
						//*****************************************************
						//*********Check if file exist and thier MD5 string****
						//*****************************************************
						String strMD5 = null;
						strMD5 = fncheckSumApacheCommons(fAllFile[i].getAbsolutePath());


						//**************************************
						//*********DB to check if file exist****
						//**************************************
						try
						{


							flag = fncheckfileExist(fAllFile[i].getAbsoluteFile().toString(), strURL, strUserName, strPassword, strMD5, fAllFile[i].getName().toString());

							String[] strArray = flag.split(":");
							int intcommand = Integer.parseInt(strArray[0]);
							String strmd5 = strArray[1];

							if(intcommand != 0)
							{



								String strencodeBase64 = null;

								try
								{
									//**************************************
									//*********File to convert Base64****
									//**************************************


									File ffile = new File(strOriginalFile);
									FileInputStream fInputstream = new FileInputStream(ffile);
									byte[] bytes = new byte[(int) ffile.length()];
									fInputstream.read(bytes);
									strencodeBase64 = new String(Base64.getEncoder().encodeToString(bytes));


									//**************************************
									//*********Do Restfull PUT call to ELK**
									//**************************************

									if (strencodeBase64.length()>0)
									{
										String StrDocID = "DocID_" + i + timeMilli;
										RestHighLevelClient client = new RestHighLevelClient(
												RestClient.builder(
														new HttpHost(strElkHost, intElkPort, strProto),
														new HttpHost(strElkHost, intElkPort1, strProto)));
										System.out.println( "Job Initiated 1" + client );					
										System.out.println( "Job Initiated 1" + StrDocID );	
										System.out.println( "Job Initiated 1" + strElkHost );	

										fnsetContent(client, strencodeBase64, StrDocID, strIndexName , strPipeline);
										//fngetContent(client, StrDocID, strIndexName );
										client.close();
										fInputstream.close();

										//**************************************
										//*********Code to insert**
										//**************************************
										//
										fnInsertNewFile(fAllFile[i].getAbsoluteFile().toString(), strURL, strUserName, strPassword, strMD5, fAllFile[i].getName().toString(), StrDocID, strIndexName, stDate);

									}
								} 
								catch(IOException e)
								{

								}
							}


						}
						catch(SQLException e)
						{}


					}

				}

			}
		}
		catch(IOException e)
		{
		}

		System.out.println( "Job Completed" );

	}



	public static void fnsetContent(RestHighLevelClient client, String strBase64Encoded, String strdocid, String strIdxName, String strstrPipeline) throws IOException 

	{

		//**************************************************
		//*********Method to Call Restful API to do insert**
		//**************************************************

		try {
		String docId = strdocid;
		IndexRequest request = new IndexRequest(strIdxName);
		request.id(docId);
		String jsonString = "{" +
				"\"data\":\"" + strBase64Encoded + "\"" +
				"}";
		request.setPipeline(strstrPipeline);
		request.source(jsonString, XContentType.JSON);
		IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
		System.out.println( "uploaded=>"+ strdocid );
		}
		catch(Exception e)
		{
			
		}
	}

	//Optional
	public static String fngetContent(RestHighLevelClient client, String strdocid,  String strIdxName) throws IOException {

		//**************************************************
		//*********Method to Call Restful API to do retrieve*
		//**************************************************


		GetRequest getRequest = new GetRequest(
				strIdxName, 
				strdocid);
		GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
		//System.out.println(getResponse.toString());
		return getResponse.toString();

	}

	public static List<File> fnfetechListFilesandDirectory(String strdirectory) throws IOException
	{

		//**************************************************
		//*******Retrieve File from NAS*********************
		//**************************************************

		File objDir = new File(strdirectory);

		List<File> result = new ArrayList<File>();
		File[] fList = objDir.listFiles();
		result.addAll(Arrays.asList(fList));

		if(fList != null)
		{
			for (File file : fList)
			{
				if(file.isFile())
				{
					files.add(file);

				}
				else if(file.isDirectory())
				{
					result.addAll(fnfetechListFilesandDirectory(file.getAbsolutePath()));
				}
			}
		}
		return result;
	}

	private static String fncheckfileExist(String strfilePath, String strURL, String strUserName, String strPassword, String strMD5Value, String strFilenamevalue)  throws SQLException
	{

		String strReturnvalue =  "1".toString() +":" + "001";
		try
		{
			int intCount = 0;
			String strFilename = null;
			String strmd5value = null;
			databaseclass objdb = new databaseclass();
			Connection conn = objdb.startConnection(strURL, strUserName, strPassword);
			final String strqueryCheck = "SELECT count(*) as Total, filename, md5value from filerepo WHERE filepath = ? AND md5value =?";
			//System.out.println("Final Query" + strqueryCheck);
			//System.out.println("Final FilePath" + strfilePath);
			//System.out.println("Final FilePath" + strMD5Value);
			final PreparedStatement ps = conn.prepareStatement(strqueryCheck);
			ps.setString(1, strfilePath);
			ps.setString(2, strMD5Value); 
			final ResultSet resultSet = ps.executeQuery();
			if (resultSet.next())
			{
				intCount = resultSet.getInt("Total");
				strFilename = resultSet.getString("filename");
				strmd5value = resultSet.getString("md5value");
				
			}


			if (intCount > 0)
			{
				if (strFilename.equals(strFilenamevalue) && strmd5value.equals(strMD5Value))
				{

					strReturnvalue = "0".toString() + ":" + "000";
					//return strReturnvalue;

				}
				else if (strFilename.equals(strFilenamevalue) && !strmd5value.equals(strMD5Value))
				{
					strReturnvalue = "2".toString() + ":" + strMD5Value;
					//return strReturnvalue;
				}
			}
			else
			{
				strReturnvalue = "1".toString() +":" + "001";
			}
			

			objdb.closeConnection(conn);	
		}
		catch (SQLException e)
		{
			//System.out.println(e.getMessage());

			
		}
		
		return strReturnvalue; 

	}

	private static void fnInsertNewFile(String strfilePath, String strURL, String strUserName, String strPassword, String stringMD5, String strFilename, String strDocid, String strIndexName, Date stDate )  throws SQLException
	{
		try

		{

			databaseclass objdb = new databaseclass();
			Connection conn = objdb.startConnection(strURL, strUserName, strPassword);
			String flag = fncheckfileExist (strfilePath, strURL, strUserName, strPassword, stringMD5, strFilename);

			//if (!flag)
			//{	
			//	final String strqueryInsert = "INSERT INTO filerepo (filename, md5value, filepath, updateddt, DocID, IndexName) VALUES (?,?,?,?,?,?)";

			//	final PreparedStatement ps = conn.prepareStatement(strqueryInsert); 
			//	ps.setString(1,strFilename);
			//	ps.setString(2, stringMD5);
			//	ps.setString(3, strfilePath);
			//	ps.setDate(4,  stDate);
			//	ps.setString(5, strDocid);
			//	ps.setString(6, strIndexName);
			//	ps.execute(); 
			//	objdb.closeConnection(conn);
			//}

			String[] strArray = flag.split(":");
			int intcommand = Integer.parseInt(strArray[0]);
			String strmd5 = strArray[1];

			switch (intcommand)

			{
			case 1:
				final String strqueryInsert = "INSERT INTO filerepo (filename, md5value, filepath, updateddt, DocID, IndexName) VALUES (?,?,?,?,?,?)";

				final PreparedStatement ps = conn.prepareStatement(strqueryInsert); 
				ps.setString(1,strFilename);
				ps.setString(2, stringMD5);
				ps.setString(3, strfilePath);
				ps.setDate(4,  stDate);
				ps.setString(5, strDocid);
				ps.setString(6, strIndexName);
				ps.execute(); 
				objdb.closeConnection(conn);
				ps.close();
				break;

			case 2:  

				final String strqueryudate = "UPDATE filerepo SET md5value = ? , updateddt = ? where filename = ? ";
				final PreparedStatement ps1 = conn.prepareStatement(strqueryudate); 
				ps1.setString(2, stringMD5);
				ps1.setString(3, strfilePath);
				ps1.setDate(4,  stDate);
				ps1.execute(); 
				objdb.closeConnection(conn);
				ps1.close();
				break;

			default:

				objdb.closeConnection(conn);
				break;

			}


		}
		catch (SQLException e)
		{
			//System.out.println(e.getMessage());
		}

	}

	public static String fncheckSumApacheCommons(String file){
		File ffile = new File(file);
		String checksum = null;
		try {  
			checksum = DigestUtils.md5Hex(new FileInputStream(ffile));
		} catch (IOException ex) {
			//logger.log(Level.SEVERE, null, ex);
		}
		return checksum;
	}



}
