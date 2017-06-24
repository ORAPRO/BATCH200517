package com.laboros.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.WordCountMapper;
import com.laboros.reducer.WordCountReducer;

public class WordCountDriver extends Configured implements Tool {

	public static void main(String[] args) {
		
		System.out.println("MAIN METHOD");
		//Step: 1 Validations input and output provided
		if(args.length!=2)
		{
			System.out.println("JAVA USAGE "+WordCountDriver.class.getName()+" /hdfs/input/file /path/to/hdfs/destination/directory");
			return;
		}
		
		//Step: 2 Load Configuration
		
		Configuration conf = new Configuration(Boolean.TRUE);
		
		try {
			int i= ToolRunner.run(conf, new WordCountDriver(), args);
			
			if(i==0){
				System.out.println("SUCCESS");
			}else{
				System.out.println("FAILURE");
			}
		} catch (Exception e) 
		{
			System.out.println("FAILURE");
			e.printStackTrace();
		}
	}

	
	@Override
	public int run(String[] args) throws Exception {
		
		//Steps =10
		//Step-1 : Getting the configuration
		Configuration conf = super.getConf();
		
		//step-2: Creating the job instance
		Job wordCountDriver =Job.getInstance(conf, WordCountDriver.class.getName());
		
		//step-3 : setting the mapper classpath for the client.jar
		wordCountDriver.setJarByClass(WordCountDriver.class);
		//step-4 : Setting input
		final String hdfsInput = args[0];
		final Path hdfsInputPath = new Path(hdfsInput);
		TextInputFormat.addInputPath(wordCountDriver, hdfsInputPath);
		wordCountDriver.setInputFormatClass(TextInputFormat.class);
		
		//step-5: Setting output
		final String hdfsOuputDir = args[1];
		final Path hdfsOuputDirPath = new Path(hdfsOuputDir);
		TextOutputFormat.setOutputPath(wordCountDriver, hdfsOuputDirPath);
		wordCountDriver.setOutputFormatClass(TextOutputFormat.class);
		//step-6: Setting mapper
		wordCountDriver.setMapperClass(WordCountMapper.class);
		//step-7: Setting mapper output key and value classes
		wordCountDriver.setMapOutputKeyClass(Text.class);
		wordCountDriver.setMapOutputValueClass(IntWritable.class);
		//step-8: setting reducer
		wordCountDriver.setReducerClass(WordCountReducer.class);
		//step-9: Setting reducer output key and value classes
		wordCountDriver.setOutputKeyClass(Text.class);
		wordCountDriver.setOutputValueClass(IntWritable.class);
		//step: trigger method
		wordCountDriver.waitForCompletion(Boolean.TRUE);
		return 0;
	}
}
