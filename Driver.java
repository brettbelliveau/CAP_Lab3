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
import org.apache.hadoop.io.DoubleWritable.Comparator;
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
		//job5(args);
		
		copyFiles(args);

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
				conf.set("NumberOfItems", "N=20000");
				
				Job job = Job.getInstance(conf);
				job.setJarByClass(Driver.class);
	
				job.setMapperClass(MapperJob5.class);
				job.setReducerClass(ReducerJob5.class);
				
				job.setSortComparatorClass(ReverseComparator.class);
				
				job.setOutputKeyClass(DoubleWritable.class);
				job.setOutputValueClass(Text.class);
				
				if (i == 0)
					FileInputFormat.addInputPath(job, new Path(args[1] + "/temp/belliveau/iter1"));
				
				else if (i == 1)
					FileInputFormat.addInputPath(job, new Path(args[1] + "/temp/belliveau/iter8"));
				
				job.setInputFormatClass(TextInputFormat.class);
				
				FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp/belliveau/job5/p" + (i+1)));
				job.setOutputFormatClass(TextOutputFormat.class);
	
				job.setNumReduceTasks(1);
				job.waitForCompletion(true);
			} catch (Exception e) {
				System.err.println("Error during job five.");
				e.printStackTrace();
			}
		}
	}
	
	static class ReverseComparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public ReverseComparator() {
            super(Text.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                return (-1)* TEXT_COMPARATOR
                        .compare(b1, s1, l1, b2, s2, l2);
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof Text && b instanceof Text) {
                return (-1)*(((Text) a)
                        .compareTo((Text) b));
            }
            return super.compare(a, b);
        }
    }	
	
	public void copyFiles(String[] args){
		try {
			Configuration conf = new Configuration();
			
			//File one
			String the_string_path_to_src = args[1] + "/temp/belliveau/job2/part-r-00000";
			String the_string_path_to_dst = args[1] + "/results/PageRank.outlink.out";
			Path srcPath = new Path(the_string_path_to_src);
			Path dstPath = new Path(the_string_path_to_dst);
			FileSystem fs_src = srcPath.getFileSystem(conf);
			FileSystem fs_dst = dstPath.getFileSystem(conf);
			FileUtil.copy(fs_src, srcPath, fs_dst, dstPath, false, conf);
	
			//File two
			the_string_path_to_src = args[1] + "/temp/belliveau/job5/p1/part-r-00000";
			the_string_path_to_dst = args[1] + "/results/PageRank.iter1.out";
			srcPath = new Path(the_string_path_to_src);
			dstPath = new Path(the_string_path_to_dst);
			fs_src = srcPath.getFileSystem(conf);
			fs_dst = dstPath.getFileSystem(conf);
			FileUtil.copy(fs_src, srcPath, fs_dst, dstPath, false, conf);
			

			//File three
			the_string_path_to_src = args[1] + "/temp/belliveau/job5/p2/part-r-00000";
			the_string_path_to_dst = args[1] + "/results/PageRank.iter8.out";
			srcPath = new Path(the_string_path_to_src);
			dstPath = new Path(the_string_path_to_dst);
			fs_src = srcPath.getFileSystem(conf);
			fs_dst = dstPath.getFileSystem(conf);
			FileUtil.copy(fs_src, srcPath, fs_dst, dstPath, false, conf);

			//File four
			the_string_path_to_src = args[1] + "/temp/belliveau/job3/part-r-00000";
			the_string_path_to_dst = args[1] + "/results/PageRank.n.out";
			srcPath = new Path(the_string_path_to_src);
			dstPath = new Path(the_string_path_to_dst);
			fs_src = srcPath.getFileSystem(conf);
			fs_dst = dstPath.getFileSystem(conf);
			FileUtil.copy(fs_src, srcPath, fs_dst, dstPath, false, conf);
			
		}
			
		catch(Exception e) { 
			e.printStackTrace(); 
		}
	}
}