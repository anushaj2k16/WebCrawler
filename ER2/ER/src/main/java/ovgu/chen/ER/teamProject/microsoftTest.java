// // This sample uses the Apache HTTP client from HTTP Components (http://hc.apache.org/httpcomponents-client-ga/)
package ovgu.chen.ER.teamProject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.net.URI;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.enums.Enum;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class microsoftTest 
{
	private static microsoftTest jsonreqobj=new microsoftTest();
	private  String vertexFileName;
	private  String edgeFileName;
	private  String authorFileName;
	private  String UpstreamFileName;
	private  StringBuilder sb;
	DateTimeFormatter uniqueId_ts = DateTimeFormatter.ofPattern("yyyyMMddhhmmss");
	DateTimeFormatter dtf_ts = DateTimeFormatter.ofPattern("yyyy-MM-ddHH:mm:ss");
	private  FileWriter vertexFW;
	private int i=0;
	private  FileWriter edgeFW;
	private  FileWriter upstreamFW;
	private  FileWriter authorFW;
	private String authorNames; 
	private static int hop=2;
	private static Map<Object, Object> idsToVisit;
	private static Map<Object, Object> idsToVisitCopy;
	private static int idKey=0;
	static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH-mm");
	static LocalDateTime now = LocalDateTime.now();
    public static void main(String[] args) 
    {
    	
    	String JSONResult_seed;
    	String JSONResult;
    	//JSONResult_seed= jsonreqobj.getData("Composite(AA.AuN=='michael stonebraker')" ,"Id,RId,Ti,Y,CC,AA.AuN,AA.AuId","10");  
    	JSONResult_seed= jsonreqobj.getData("And(And(Ti='a relational model of data for large shared data banks',Composite(AA.AuN=='e f codd')),Y=1970)" ,"Id,RId,Ti,Y,CC,AA.AuN,AA.AuId","10");
    	System.out.println("Indexing started");
    	jsonreqobj.indexVertex(JSONResult_seed); 		
        hop++;
    	
        int count=0;
        idsToVisitCopy = new HashMap<>();
        idsToVisitCopy.putAll(idsToVisit);
	   	 Iterator it = idsToVisitCopy.entrySet().iterator();
	   	 while (it.hasNext()) {
	   	 Map.Entry pair = (Map.Entry)it.next();
	   	// System.out.println(pair.getKey() + " = " + pair.getValue());
	   	 JSONResult= jsonreqobj.getData("Id="+pair.getValue().toString() ,"Id,RId,Ti,Y,CC,AA.AuN,AA.AuId","1");
	   	 jsonreqobj.indexVertex(JSONResult); 
	   	 System.out.println(count++);
	    }
     System.out.println("Indexing ends");
    }


	private  String getData(String expression, String attributes, String count) {
		HttpClient httpclient = HttpClients.createDefault();       
		String JSONResult=null;
        try
        {
        	  URIBuilder builder = new URIBuilder("https://westus.api.cognitive.microsoft.com/academic/v1.0/evaluate");
              builder.setParameter("expr", expression);
              builder.setParameter("model", "latest");
              builder.setParameter("attributes", attributes );
              builder.setParameter("count", count);
        	
              URI uri = builder.build();
              HttpGet request = new HttpGet(uri);
              request.setHeader("Ocp-Apim-Subscription-Key", "dbe029f01ce145f5a41390c981f3bfc5");


            // Request body
            HttpUriRequest reqEntity = request;    
            HttpResponse response = httpclient.execute(reqEntity);
            HttpEntity entity = response.getEntity();

            if (entity != null) 
            {
              JSONResult=EntityUtils.toString(entity);
            System.out.println(JSONResult);
       		 
            }
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
        return JSONResult;
	}
    
    
	
    
	 public void indexVertex(String jsonArray){	
			sb = new StringBuilder();
			idsToVisit = new HashMap<>();
		   if (vertexFileName == null) {
				vertexFileName = "papers" + dtf.format(now) + ".csv";
				try {
					vertexFW = new FileWriter(new File(vertexFileName), true);
					sb.append("UniqueId");
					sb.append(',');
					sb.append("PaperId");
					sb.append(',');
					sb.append("Title");
					sb.append(',');
					sb.append("PublishInYear");
					sb.append(',');
					sb.append("TimestampAdded");
					sb.append(',');
					sb.append("TimestampLastVisited");
					sb.append(',');
					sb.append("TimestampMod");
					//sb.append(',');
					//sb.append("Author Names");
					//sb.append(',');
					//sb.append("Count");
					//sb.append(',');
					//sb.append("PublishInYear");
					vertexFW.write(System.getProperty("line.separator"));
					vertexFW.write(sb.toString());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}			
			}
		   
		   // String authorNames;
		   // int count=0;
		   		JsonObject root = new JsonParser().parse(jsonArray).getAsJsonObject();
		   		JsonArray jsonarray = root.getAsJsonArray("entities");
		   		for(JsonElement json:jsonarray){
		   			//count++;
		   			//authorNames=new String();
		   				   					   			
		   	//	System.out.println(json.getAsJsonObject());
		   	//	System.out.println(json.getAsJsonObject().get("Id"));
		   	//	System.out.println(json.getAsJsonObject().get("Ti"));
		   	//	System.out.println(json.getAsJsonObject().get("CC"));
		   	//	System.out.println(json.getAsJsonObject().get("Y"));
		   			   		
		   		//JsonObject author = json.getAsJsonObject();
		   		//JsonArray authorarray = author.getAsJsonArray("AA");
		   		
		   	/*	for(JsonElement author_json:authorarray){
		   			authorNames=authorNames+author_json.getAsJsonObject().get("AuN");
		   		}*/
		   		
		   		try {
			   		LocalDateTime now = LocalDateTime.now();
		   			sb=new StringBuilder();
		   			sb.append(uniqueId_ts.format(now)+"-"+"001"+"-"+"01"+"-"+((i++)%99));
			   		sb.append(',');
		   			sb.append(json.getAsJsonObject().get("Id"));
			   		sb.append(',');
			   		sb.append(json.getAsJsonObject().get("Ti").toString().replaceAll("^\"|\"$", ""));
			   		sb.append(',');
			   		sb.append(json.getAsJsonObject().get("Y"));
			   		sb.append(','); 		
			   		sb.append(dtf_ts.format(now));
			   		sb.append(",");
			   		sb.append(dtf_ts.format(now));
			   		sb.append(",");
			   		sb.append(dtf_ts.format(now));
			   		sb.append(",");
			   		//sb.append(authorNames);
			   		//sb.append(',');
			   		//sb.append(json.getAsJsonObject().get("CC"));
			   		//sb.append(',');
			   		//sb.append(json.getAsJsonObject().get("Y"));
			   		vertexFW.write(System.getProperty("line.separator"));
			   		vertexFW.write(sb.toString());	
			   		jsonreqobj.indexAuthor(json.getAsJsonObject().get("Id").toString(), jsonArray);
						String JSONResult_edges;
						String RId= "RId="+json.getAsJsonObject().get("Id").toString() ;
						String citationCount=json.getAsJsonObject().get("CC").toString();
						JSONResult_edges=getData(RId, "Id", citationCount);
						//Thread.sleep(5000);
						jsonreqobj.indexEdges(json.getAsJsonObject().get("Id").toString(),JSONResult_edges); 			  		
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}/* catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}*/
		   		
			  } 
			 /* try {
				  vertexFW.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} */
			//  System.out.println(count);
	 }
	 
	 public void indexEdges(String fromPaperId,String jsonArray){
		  
		 sb = new StringBuilder();			
		   if (edgeFileName == null) {
				edgeFileName = "cited_by" + dtf.format(now) + ".csv";
				try {
					edgeFW = new FileWriter(new File(edgeFileName), true);
					sb.append("UniqueId");
					sb.append(',');
					sb.append("FromPaperId");
					sb.append(',');
					sb.append("CitedPaperId");
					sb.append(',');
					sb.append("TimeStamp");
					edgeFW.write(System.getProperty("line.separator"));
					edgeFW.write(sb.toString());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}			
			}
		   
		   JsonObject root = new JsonParser().parse(jsonArray).getAsJsonObject();
	   		JsonArray jsonarray = root.getAsJsonArray("entities");
	   		for(JsonElement json:jsonarray){
	   			try {
	   				if(hop==2){
	   					idKey++;
		   				idsToVisit.put(idKey, json.getAsJsonObject().get("Id").toString());
	   				}
	   				
		   			sb=new StringBuilder();
		   			sb.append(uniqueId_ts.format(now)+"-"+"001"+"-"+"01"+"-"+((i++)%99));
			   		sb.append(',');
		   			sb.append(fromPaperId);
			   		sb.append(',');
			   		sb.append(json.getAsJsonObject().get("Id"));
			   		sb.append(',');
			   		DateTimeFormatter dtf_ts = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			   		LocalDateTime now = LocalDateTime.now();
			   		sb.append(dtf_ts.format(now));
			   		edgeFW.write(System.getProperty("line.separator"));
			   		edgeFW.write(sb.toString());						
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		   }
	 }
	 
	 public String getUniqueId(){
		return  (uniqueId_ts.format(now)+"-"+"001"+"-"+"01"+"-"+((i++)%99));
	 }

	 public void indexAuthor(String fromPaperId,String jsonArray){
		 authorNames=new String();
		 sb = new StringBuilder();			
		   if (authorFileName == null) {
			   authorFileName = "author" + dtf.format(now) + ".csv";
				try {
					authorFW = new FileWriter(new File(authorFileName), true);
					sb.append("UniqueId");
					sb.append(',');
					sb.append("PaperId");
					sb.append(',');
					sb.append("Author Name");
					authorFW.write(System.getProperty("line.separator"));
					authorFW.write(sb.toString());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}			
			}
		   
		   JsonObject root = new JsonParser().parse(jsonArray).getAsJsonObject();
	   		JsonArray jsonarray = root.getAsJsonArray("entities");
	   		for(JsonElement json:jsonarray){
	   			
	   			JsonObject author = json.getAsJsonObject();
	   			JsonArray authorarray = author.getAsJsonArray("AA");
		   		
			   	for(JsonElement author_json:authorarray){
			   			authorNames=authorNames+author_json.getAsJsonObject().get("AuN").toString().replaceAll("^\"|\"$", "");
			   			authorNames=authorNames+",";
			   		}		
			   	authorNames=authorNames.substring(0,authorNames.length()-1);
	   			try {
	   				   				
		   			sb=new StringBuilder();
		   			sb.append(uniqueId_ts.format(now)+"-"+"001"+"-"+"01"+"-"+((i++)%99));
			   		sb.append(',');
		   			sb.append(fromPaperId);
			   		sb.append(',');
			   		sb.append(authorNames);
			   		authorFW.write(System.getProperty("line.separator"));
			   		authorFW.write(sb.toString());						
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		   }
	 }
	 
	 public void writeToUpstream(Enum operation,Enum subject, String uniqueId, JsonObject jsonObj){	  
		 sb = new StringBuilder();			
		   if (UpstreamFileName == null) {
			   UpstreamFileName = "cited_by" + dtf.format(now) + ".csv";
				try {
					upstreamFW = new FileWriter(new File(UpstreamFileName), true);
					sb.append("TimeStamp");
					sb.append(',');
					sb.append("Operation");
					sb.append(',');
					sb.append("Subject");
					sb.append(',');
					sb.append("Target");
					sb.append(',');
					sb.append("Details");
					upstreamFW.write(System.getProperty("line.separator"));
					upstreamFW.write(sb.toString());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}			
			}		
		   			sb=new StringBuilder();
		   			sb.append(uniqueId_ts.format(now)+"-"+"001"+"-"+"01"+"-"+((i++)%99));
			   		sb.append(',');
		   			sb.append(operation);
			   		sb.append(',');
			   		sb.append(subject);
			   		sb.append(',');
			   		sb.append(uniqueId);
			   		sb.append(',');
			   		sb.append(jsonObj);
			   		try {
						upstreamFW.write(System.getProperty("line.separator"));
				   		upstreamFW.write(sb.toString());	
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
								
						
					} 
		    
	 public enum operation{
		 CREATE,
		 UPDATE,
		 DELETE;
	 }
	 
	 public enum subject{
		 PAPERS,
		 CITED_BY,
		 AUTHORS;
	 }
}