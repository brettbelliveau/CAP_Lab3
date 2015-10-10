import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJob2 extends Reducer<Text, Text, Text, Text> {
	
    private Text link = new Text();
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
    	ArrayList<String> list = new ArrayList<>();
    	
    	for(Text value : values)
    		list.add(value.toString());
    	
    	String str = "";
    	
        for (int i = 0; i < list.size(); i++){
        	if (list.get(i).equals(key.toString())) {
        		//do nothing
        	}
        	else if (!str.contains(list.get(i))){
        		str += list.get(i);
            	str += '\t';
        	}
        }
    	link.set(str);
		context.write(key, link);
    }
}