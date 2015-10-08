import java.io.IOException;
import java.util.ArrayList;
import java.io.File;

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
import org.apache.mahout.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.dom4j.*;

public class Driver extends Configured implements Tool {
    
	//Gloabl Variables
	private ArrayList<ArrayList<String>> graph = new ArrayList<ArrayList<String>>();
    
	private int N = 0; //total number of pages

    private PageRank pr = new PageRank();
	
    //Main method: Calls run and exits
    public static void main(String[] args) throws Exception {
    	int result = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(result);
    }
    
    //Run method: 
    public int run(String args[]) {
    	//Have something here eventually
    	
        job1(args);
        //job2(args);
        //job3(args);
        //job4(args);
        //job5(args);
        
        return 0;
    }
    
    //Extracts wikilinks and removes red links
    public void job1(String args[]) {
       	try {
       		
       		Configuration conf = new Configuration();
       		conf.set("xmlinput.start", "<page>"); //set the start tag
       		conf.set("xmlinput.end", "</page>"); // set the end tag
       		
       		Job job = Job.getInstance(conf, "WikiGraph");
       		job.setJarByClass(ExtractTitlesLinks.class);
       		job.setOutputKeyClass(Text.class);
       		job.setOutputValueClass(Text.class);
       		job.setMapperClass(MRMapper.class);
       		job.setReducerClass(MRReducer.class);
       		job.setInputFormatClass(XMLInputFormat.class); // tell hadoop to use mahout XmlInputFormat (instead of TextInputFormat)
       		job.setOutputFormatClass(TextOutputFormat.class);
       		FileInputFormat.addInputPath(job, new Path(args[0]));
       		FileOutputFormat.setOutputPath(job, new Path(args[1]));
       		job.setNumReduceTasks(1);
       		job.waitForCompletion(true);
       		/*
       	
            Configuration conf = new Configuration();
            
            File parsedXml = new File(pr.parseXml(args));
            
            Job job = Job.getInstance(conf);
            job.setJarByClass(Driver.class);

            job.setMapperClass(MRMapper.class);

            job.setReducerClass(MRReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //FileInputFormat.addInputPath(job, new Path(args[0]));
            FileInputFormat.addInputPath(job, new Path(parsedXml.toPath().toString())); 
            job.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.waitForCompletion(true);
            */
        } catch (Exception e) {
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