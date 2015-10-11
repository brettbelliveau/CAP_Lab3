import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJob4 extends Reducer<Text, Text, Text, Text> {
	
	private int iteration;
	private double N;
	
	private Text scoreT = new Text();
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    	Configuration conf = context.getConfiguration();
    	String iterstr = conf.get("iteration");
    	String Nstr = conf.get("NumberOfItems");
    	
    	iteration = Integer.parseInt(iterstr);
    	String split[] = Nstr.split("=");
    	split[1] = split[1].trim();
    	N = Double.parseDouble(split[1]);
    	
    	ArrayList<String> list = new ArrayList<>();
    	
    	for(Text value : values)
    		list.add(value.toString());
    	
        double score = ((1 - 0.85) / N); //Initial score for all
        String currentItem = "";
        double itemsum = 0;
        
        for (int index = 0; index < list.size(); index++){
	        String line = list.get(index);
	        StringTokenizer str = new StringTokenizer(line);
	        currentItem = str.nextToken();
	        double currentItemScore = 0;
	        if (currentItem.contains(key.toString())) //dummy item
	        {}
	        else {
	        	if (iteration > 1) { //we have a score for item
	        		String currentItemScoreStr = conf.get(currentItem);
	        		currentItemScore = Double.parseDouble(currentItemScoreStr);
	        	}
	        	else
	        		currentItemScore = (1/N);
	        	
	        	
	        	currentItemScore /= Double.parseDouble(str.nextToken());
	        	itemsum += currentItemScore;
	        }
    	}
        score += itemsum*(0.85);

        scoreT.set("" + score);
        context.write(key, scoreT);
	}
}
