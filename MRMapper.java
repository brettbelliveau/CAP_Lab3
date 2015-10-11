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
        	String split2[] = split[1].split("\\]\\]", 2);
        	
        	if ((split2[0].contains("[["))) {
        	
	        	while (split2[0].contains("[[")){ //nested link(s)
	        		
	            	String splitinner[] = split2[0].split("\\[\\[", 2);
	        		
	        		String linkstr = extractLink(splitinner[1]);
	            	
	            	linkstr = linkstr.replaceAll(" ", "_");
	            	link.set(linkstr);
	        	
	        		context.write(link, title);		
	
	            	split2 = split2[1].split("\\]\\]", 2);
	        	}
	        	if (split2.length > 1)
	        		body = split2[1];
	        	else
	        		body = "";
        	}
        	
        	else {
        		String linkstr = extractLink(split2[0]);
            	
            	linkstr = linkstr.replaceAll(" ", "_");
            	link.set(linkstr);
        	
        		context.write(link, title);		

	    		if (split2.length > 1)
	        		body = split2[1];
	    		else
	        		body = "";
        	}
        }
    }
    
    public String extractLink(String body) {
    	
    	if (body.contains("|")){
    		String thirdsplit[] = body.split("\\|", 2);
    		body = thirdsplit[0];
    	}

    	return body;
    }
}