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
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Driver extends Configured implements Tool {

	// Gloabl Variables
	private String N = ""; // total number of pages

	// Main method: Calls run and exits
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(result);
	}

	// Run method:
	public int run(String args[]) {
		// Have something here eventually

		//job1(args);
		//job2(args);
		//job3(args);
		//job4(args);
		job5(args);

		return 0;
	}

	// Extracts wikilinks and removes red links
	public void job1(String args[]) {
		try {

			Configuration conf = new Configuration();
			conf.set("xmlinput.start", "<page>"); // set the start tag
			conf.set("xmlinput.end", "</page>"); // set the end tag

			Job job = Job.getInstance(conf, "WikiGraph");
			job.setJarByClass(ExtractTitlesLinks.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(MRMapper.class);
			job.setReducerClass(MRReducer.class);
			job.setInputFormatClass(XMLInputFormat.class); // tell hadoop to use
															// mahout
															// XmlInputFormat
															// (instead of
															// TextInputFormat)
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp/belliveau/iter0"));
			job.setNumReduceTasks(1);
			job.waitForCompletion(true);

		} catch (Exception e) {
			System.err.println("Error during job one.");
			e.printStackTrace();
		}
	}

	// Generates the outlink adjacency graph
	public void job2(String args[]) {
		try {
			Configuration conf = new Configuration();

			Job job = Job.getInstance(conf);
			job.setJarByClass(Driver.class);

			job.setMapperClass(MapperJob2.class);
			job.setReducerClass(ReducerJob2.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[1] + "/temp/belliveau/iter0"));
			job.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp/belliveau/job2"));
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setNumReduceTasks(1);
			job.waitForCompletion(true);

		} catch (Exception e) {
			System.err.println("Error during job two.");
			e.printStackTrace();
		}
	}

	// Computes the total number of pages (denoted as N)
	public void job3(String args[]) {
		try {
			Configuration conf = new Configuration();

			Job job = Job.getInstance(conf);
			job.setJarByClass(Driver.class);

			job.setMapperClass(MapperJob3.class);
			job.setReducerClass(ReducerJob3.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[1] + "/temp/belliveau/job2"));
			job.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp/belliveau/job3"));
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setNumReduceTasks(1);
			job.waitForCompletion(true);

			Path path = new Path(args[1] + "/temp/belliveau/job3/part-r-00000");
			FileSystem fs = path.getFileSystem(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;
			line = br.readLine();
			if (line != null) {
				N = line;
			}

		} catch (Exception e) {
			System.err.println("Error during job three.");
			e.printStackTrace();
		}
	}

	// Performs PageRank for 8 iterations (input link graph, output link graph)
	public void job4(String args[]) {

		for (int i = 0; i < 8; i++) {
			try {
				Configuration conf = new Configuration();
				conf.set("iteration", "" + (i + 1));
				conf.set("NumberOfItems", N);

				if (i > 0) { // Time to set all the scores
					Path path = new Path(args[1] + "/temp/belliveau/iter" + i + "/part-r-00000");
					FileSystem fs = path.getFileSystem(conf);
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
					String line;
					String link = "";
					String score = "";
					line = br.readLine();
					while (line != null) {
						StringTokenizer str = new StringTokenizer(line);
						if (str.hasMoreTokens()) {
							link = str.nextToken();
							if (str.hasMoreTokens())
								score = str.nextToken();
						}
						conf.set(link, score);
						line = br.readLine();
					}
				}
				Job job = Job.getInstance(conf);
				job.setJarByClass(Driver.class);

				job.setMapperClass(MapperJob4.class);
				job.setReducerClass(ReducerJob4.class);

				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);

				FileInputFormat.setInputPaths(job, new Path(args[1] + "/temp/belliveau/job2"));
				job.setInputFormatClass(TextInputFormat.class);

				FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp/belliveau/iter" + (i + 1)));
				job.setOutputFormatClass(TextOutputFormat.class);

				job.setNumReduceTasks(1);
				job.waitForCompletion(true);

			} catch (Exception e) {
				System.err.println("Error during job four, iteration " + i);
				e.printStackTrace();
			}
		}
	}

	// Prints readable list of article names and PageRank scores (desc) from
	// link graph
	// Gonna need some witchcraft for this bad boy
	public void job5(String args[]) {
		for (int i = 0; i < 2; i++) {
			try {
				Configuration conf = new Configuration();
				conf.set("iteration", ""+(i+1));
				conf.set("NumberOfItems", "N=20000");
				
				Job job = Job.getInstance(conf);
				job.setJarByClass(Driver.class);
	
				job.setMapperClass(MapperJob5.class);
				job.setReducerClass(ReducerJob5.class);
	
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				
				if (i == 0)
					FileInputFormat.addInputPath(job, new Path(args[1] + "/temp/belliveau/iter1"));
				
				else if (i == 1)
					FileInputFormat.addInputPath(job, new Path(args[1] + "/temp/belliveau/iter8"));
				
				job.setInputFormatClass(TextInputFormat.class);
				
				FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp/belliveau/Job5/p" + (i+1)));
				job.setOutputFormatClass(TextOutputFormat.class);
	
				job.setNumReduceTasks(1);
				job.waitForCompletion(true);
			} catch (Exception e) {
				System.err.println("Error during job five.");
				e.printStackTrace();
			}
		}
		
		//copyFiles(args);
	}
	
	public void copyFiles(String[] args){
		try {
			
			
			
		}
		catch(Exception e) { 
			e.printStackTrace(); 
		}
	}
}