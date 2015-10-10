import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MRMapper extends Mapper<LongWritable, Text, Text, Text>{
	
    private Text title = new Text();
    private Text link = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	String line = value.toString();
        ExtractTitlesLinks extract = new ExtractTitlesLinks(line); 
    	String titlestr = extract.extractTitle();
    	String body = extract.extractBody();
    	titlestr = titlestr.replaceAll(" ", "_");
    	title.set(titlestr);
    	
    	link.set("$valid$");

    	context.write(title, link);
    	
    	while(body.contains("[[")){
        	String split[] = body.split("\\[\\[", 2);
        	
        	String secondsplit[] = split[1].split("\\]\\]", 2);
        	
        	String linkstr = "";
        	
        	if (secondsplit[0].contains("[[")){
        		String tempsplit[] = secondsplit[0].split("\\[\\[", 2);
        		secondsplit = secondsplit[1].split("\\]\\]", 2);
        		linkstr = tempsplit[1];
        	}
        	else
        		linkstr = secondsplit[0];

        	if (secondsplit.length > 1)
        		body = secondsplit[1];
        	
        	if(linkstr.contains("|")){
        		String thirdsplit[] = linkstr.split("\\|", 2);
        		linkstr = thirdsplit[0];
        		if (thirdsplit.length > 1)
        			secondsplit[1] = thirdsplit[1];
        	}
        	
        	linkstr = linkstr.replaceAll(" ", "_");
        	link.set(linkstr);
    	
    		context.write(link, title);
    	
        	if (secondsplit.length < 2)
        		break;
        }
    }
    
    public boolean isValid(String link) {
    	boolean valid = true;
    	
    	if (link.contains("#"))
    		valid = false;
    	else if (link.contains("Help:"))
    		valid = false;
    	else if (link.contains("commons:"))
    		valid = false;
    	else if (link.contains("Special:"))
    		valid = false;
    	else if (link.contains("Category:"))
    		valid = false;
    	else if (link.contains("File:"))
    		valid = false;
    	else if (link.startsWith("/"))
    		valid = false;
    	else if (link.contains("wikt:"))
    		valid = false;
    		
    	return valid;
    }
}