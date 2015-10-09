import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MRMapper extends Mapper<LongWritable, Text, Text, Text>{
	
    
	private final IntWritable one = new IntWritable(1);
    private Text title = new Text();
    private Text link = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	String line = value.toString();
        ExtractTitlesLinks extract = new ExtractTitlesLinks(line); 
    	String titlestr = extract.extractTitle();
    	String body = extract.extractBody();
    	titlestr.replaceAll(" ", "_");
    	title.set(titlestr);
    	
    	if(!body.contains("[[")){
    		link.set("$$$");
    		context.write(title, link);
    	}
    	
        while(body.contains("[[")){
        	String split[] = body.split("\\[\\[", 2);
        	String secondsplit[] = split[1].split("\\]\\]", 2);
        	
        	String linkstr = secondsplit[0];

        	if(linkstr.contains("|")){
        		String thirdsplit[] = linkstr.split("\\|", 2);
        		linkstr = thirdsplit[0];
        		secondsplit[1] = thirdsplit[1];
        	}
        	
        	linkstr.replaceAll(" ", "_");
        	
        	link.set(linkstr);
        	body = secondsplit[1];
        	
        	context.write(title, link);
        }
    }
}