package ir.control;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
	
	public class JSONReq {
		private  String vertexFileName;
		private  StringBuilder sb;
		private  FileWriter pw;
		static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH-mm");
		static LocalDateTime now = LocalDateTime.now();
		
	  public static void main(String[] args) {
		  JsonParser jsonParser = new JsonParser();
		  String JSONResult=jsonGetRequest("http://jsonplaceholder.typicode.com/posts");
		  System.out.println(JSONResult);	    
		  JsonArray jsonArray = (JsonArray) jsonParser.parse(JSONResult);
		  JSONReq jsonreqobj=new JSONReq();
		  jsonreqobj.indexData(jsonArray);
	  }

	  public void indexData(JsonArray jsonArray){	
			sb = new StringBuilder();
		    if (vertexFileName == null) {
				vertexFileName = "GraphData" + dtf.format(now) + ".csv";
				try {
					pw = new FileWriter(new File(vertexFileName), true);
					sb.append("userid");
					sb.append(',');
					sb.append("title");
					sb.append(',');
					sb.append("Timestamp");
					sb.append(',');
					sb.append("body");
					pw.write(System.getProperty("line.separator"));
					pw.write(sb.toString());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}			
			}
	
			   for(JsonElement json:jsonArray){
		    		System.out.println(json.getAsJsonObject());
		    		System.out.println(json.getAsJsonObject().get("title"));
		    		System.out.println(json.getAsJsonObject().get("userId"));
		    		System.out.println(json.getAsJsonObject().get("body"));
		    		
		    		try {
		    			sb=new StringBuilder();
		    			sb.append(json.getAsJsonObject().get("userId"));
			    		sb.append(',');
			    		sb.append(json.getAsJsonObject().get("title"));
			    		sb.append(',');
			    		DateTimeFormatter dtf_ts = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			    		LocalDateTime now = LocalDateTime.now();
			    		sb.append(dtf_ts.format(now));
			    		sb.append(",");
			    		sb.append(json.getAsJsonObject().get("body"));
						pw.write(System.getProperty("line.separator"));
						pw.write(sb.toString());						
			    		
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		    		
			   } 
			   try {
					pw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
	  }
	  
	  
	  @SuppressWarnings("resource")
	private static String streamToString(InputStream inputStream) {
	    String text = new Scanner(inputStream, "UTF-8").useDelimiter("\\Z").next();
	    return text;
	  }

	  public static String jsonGetRequest(String urlQueryString) {
	    String json = null;
	    try {
	      URL url = new URL(urlQueryString);
	      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
	      connection.setDoOutput(true);
	      connection.setInstanceFollowRedirects(false);
	      connection.setRequestMethod("GET");
	      connection.setRequestProperty("Content-Type", "application/json");
	      connection.setRequestProperty("charset", "utf-8");
	      connection.connect();
	      InputStream inStream = connection.getInputStream();
	      json = streamToString(inStream); // input stream to string
	      
	    } catch (IOException ex) {
	      ex.printStackTrace();
	    }
	    return json;
	    
	  }
	}
	
