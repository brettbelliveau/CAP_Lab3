import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MRReducer extends Reducer<Text, Text, Text, Text> {

	public Text keyt = new Text();
	public Text value = new Text();
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
    	ArrayList<String> list = new ArrayList<>();
    	
    	for(Text value : values)
    		list.add(value.toString());
    	
    	if(list.contains("$valid$")) {
        	context.write(key, key);
    		for (int i = 0; i < list.size(); i++){
       			if(!list.get(i).equals("$valid$")){
    				value.set(list.get(i));
    				context.write(key, value);
    			}
    		}
    	}
    }
}