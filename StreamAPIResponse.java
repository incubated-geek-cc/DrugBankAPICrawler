import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;

public class StreamAPIResponse {
    private static File listOfDrugsJSONFile;
    private static HashMap<String,JSONObject> all_drug_interactions;
    
    private static int callCounter;
    private static int start_threshold;
    
    public static void main(String args[]) throws FileNotFoundException, IOException {
        all_drug_interactions=new HashMap<String,JSONObject>();
        callCounter=0;
        start_threshold=0;
        
        runDataCrawling();
    }
    
    private static void runDataCrawling() throws FileNotFoundException, IOException {
        listOfDrugsJSONFile=new File("list_of_drugs.json");
        FileReader reader = new FileReader(listOfDrugsJSONFile);
        BufferedReader bufferedReader = new BufferedReader(reader);
        String input = "";
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            input += line;
        }
        reader.close();
        JSONObject jsonObj = new JSONObject(input);
        
        Set setOfDrugs=jsonObj.keySet();
        Iterator iter=setOfDrugs.iterator();
       
        while(iter.hasNext()) {
            try {
                String drugid=(String) iter.next();
                getDrugDDI(drugid,start_threshold);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }
    private static Integer getDrugDDI(String drugid, int start_threshold) throws InterruptedException, IOException {
        long unix = Instant.now().getEpochSecond();
        java.lang.Thread.sleep(6000);

        String[] infoArr=new String[3];
        infoArr[0]=drugid;
        infoArr[1]=start_threshold+"";
        infoArr[2]=unix+"";
       
        String trackerOutput="|"+( (callCounter++) +"|"+(String.join("|",infoArr))+"|" );
        String outputLog="|Request #|DrugID|Offset|Unix Timestamp|"+"\n"
                +trackerOutput+"\n";
        System.out.println(outputLog);
        
        int noOfRecords=callAPI(drugid,(start_threshold+""),(unix+""));
                
        if(noOfRecords==100) { // dataArr.length
            start_threshold+=99;
            getDrugDDI(drugid,start_threshold);
        } else {
            start_threshold=0;
        }  
        return start_threshold;
    }
    private static Integer callAPI(String drugid, String start, String unix) throws IOException {
 	String url="https://go.drugbank.com/drugs/"+drugid+"/drug_interactions.json?"
                + "group=approved&draw=4&columns%5B0%5D%5Bdata%5D=0&columns%5B0%5D%5Bname%5D=&"
                + "columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&"
                + "columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&"
                + "columns%5B1%5D%5Bdata%5D=1&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&"
                + "columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&"
                + "columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&start="+start+"&length=100&"
                + "search%5Bvalue%5D=&search%5Bregex%5D=false&_="+unix;
        
        JSONObject jsonObj=getAPIResponseObj(url);
        
        int recordsTotal=(int) jsonObj.get("recordsTotal");
        JSONArray dataArr=(JSONArray) jsonObj.get("data");

        if(all_drug_interactions.get(drugid)==null) {
            JSONObject drugEntry=new JSONObject();
            drugEntry.put("interactions", dataArr);
            drugEntry.put("recordsTotal", recordsTotal);
            all_drug_interactions.put(drugid, drugEntry);
        }
        JSONObject resultObj=new JSONObject(all_drug_interactions);
        String resultObjStr=resultObj.toString(2);
        
        FileWriter writer = new FileWriter("allDrugInteractions.json", true); // will constantly overwrite this file
        writer.write(resultObjStr);
        writer.close();

        return dataArr.length();
    }
    private static JSONObject getAPIResponseObj(String fullAPIUrl) throws IOException {
        JSONObject responseObj = null;
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpget = new HttpGet(fullAPIUrl);
        HttpResponse httpresponse;

        httpresponse = httpclient.execute(httpget);
        int statusCode = httpresponse.getStatusLine().getStatusCode();
       
        if (statusCode >= 200 && statusCode <= 299) {
            InputStream inputStream = httpresponse.getEntity().getContent();
            StringBuilder textBuilder = new StringBuilder();
            try (Reader reader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName(StandardCharsets.UTF_8.name())))) {
                int c = 0;
                while ((c = reader.read()) != -1) {
                    textBuilder.append((char) c);
                }
            }
            String jsonStrResult = textBuilder.toString();
            responseObj = new JSONObject(jsonStrResult);
        }
        return responseObj;
    }
}