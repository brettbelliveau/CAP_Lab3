import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {
    
	//Gloabl Variables
	private ArrayList<ArrayList<String>> graph = new ArrayList<ArrayList<String>>();
    
	private int N = 0; //total number of pages

    private PageRank pr = new PageRank();
	
    //Main method: Calls run and exits
    public static void main(String[] args) throws Exception {
    	int res = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(res);
    }
    
    //Run method: 
    public int run(String args[]) {
    	//Have something here eventually
    	
        job1(args);
        job2(args);
        job3(args);
        job4(args);
        job5(args);
        
        return 0;
    }
    
    //Extracts wikilinks and removes red links
    public void job1(String args[]) {
       	try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf);
            job.setJarByClass(WordCount.class);

            // specify a mapper
            job.setMapperClass(WordCountMapper.class);

            // specify a reducer
            job.setReducerClass(WordCountReducer.class);

            // specify output types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // specify input and output DIRECTORIES
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.waitForCompletion(true);
        } catch (InterruptedException|ClassNotFoundException|IOException e) {
            System.err.println("Error during job one.");
            e.printStackTrace();
        }
    }
    
    //Generates the outlink adjacency graph
    public void job2(String args[]){
    	try {
    		
	    } catch (Exception e) {
	        System.err.println("Error during job two.");
	        e.printStackTrace();
	    }
    }
    
    //Computes the total number of pages (denoted as N)
    public void job3(String args[]){
    	try {
    		
	    } catch (Exception e) {
	        System.err.println("Error during job three.");
	        e.printStackTrace();
	    }
    }

    //Performs PageRank for 8 iterations (input link graph, output link graph)
    public void job4(String args[]){
    	try {
    		
	    } catch (Exception e) {
	        System.err.println("Error during job four.");
	        e.printStackTrace();
	    }
    }

    //Prints readable list of article names and PageRank scores (desc) from link graph
    public void job5(String args[]){
    	try {
    		
	    } catch (Exception e) {
	        System.err.println("Error during job five.");
	        e.printStackTrace();
	    }
    }
}