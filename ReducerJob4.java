import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJob4 extends Reducer<Text, Text, Text, Text> {
	
	private Text links = new Text();

	private int index;
	private int iteration;
	private int N;
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    	Configuration conf = context.getConfiguration();
    	String iterstr = conf.get("iteration");
    	iteration = Integer.parseInt(iterstr);
        String linkstr = "";
        double score = 0;
        index = 0;
        
        for (Text value : values){
        	linkstr += "" + value + '\t';
        	++index;
        }
        
        StringTokenizer itr = new StringTokenizer(linkstr);
        
        
        if (iteration == 1){//first run
	        if (index == 0) //there were no values
	        	score = (1-0.85)/N;
	    	
	        else{
	        	score = (1-0.85)*score; //fix this
	        }
	    }
        else //not first run
        {
        	score = Integer.parseInt(itr.nextToken());
        	while(itr.hasMoreTokens()){
        		
        	}
        }
    }
    
    public void configure(){
    	
    }
}
