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

import com.laboros.mapper.GrepMapper;
import com.laboros.reducer.WordCountReducer;

public class GrepDriver extends Configured implements Tool {

	public static void main(String[] args) {
		
		System.out.println("MAIN METHOD");
		//Step: 1 Validations input and output provided
		if(args.length<2)
		{
			System.out.println("JAVA USAGE "+GrepDriver.class.getName()+" /hdfs/input/file /path/to/hdfs/destination/directory -DSEARCH_STR");
			return;
		}
		
		//Step: 2 Load Configuration
		
		Configuration conf = new Configuration(Boolean.TRUE);
		
		try {
			int i= ToolRunner.run(conf, new GrepDriver(), args);
			
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
		
		//setup in the configuration
//		final String search_str=args[2];
//		conf.set("SEARCH_STR", search_str);
		
		//step-2: Creating the job instance
		Job grepDriver =Job.getInstance(conf, GrepDriver.class.getName());
		
		//step-3 : setting the mapper classpath for the client.jar
		grepDriver.setJarByClass(GrepDriver.class);
		//step-4 : Setting input
		final String hdfsInput = args[0];
		final Path hdfsInputPath = new Path(hdfsInput);
		TextInputFormat.addInputPath(grepDriver, hdfsInputPath);
		grepDriver.setInputFormatClass(TextInputFormat.class);
		
		//step-5: Setting output
		final String hdfsOuputDir = args[1];
		final Path hdfsOuputDirPath = new Path(hdfsOuputDir);
		TextOutputFormat.setOutputPath(grepDriver, hdfsOuputDirPath);
		grepDriver.setOutputFormatClass(TextOutputFormat.class);
		//step-6: Setting mapper
		grepDriver.setMapperClass(GrepMapper.class);
		//step-7: Setting mapper output key and value classes
		grepDriver.setMapOutputKeyClass(Text.class);
		grepDriver.setMapOutputValueClass(IntWritable.class);
		//step-8: setting reducer
		grepDriver.setReducerClass(WordCountReducer.class);
		//step-9: Setting reducer output key and value classes
		grepDriver.setOutputKeyClass(Text.class);
		grepDriver.setOutputValueClass(IntWritable.class);
		//step: trigger method
		grepDriver.waitForCompletion(Boolean.TRUE);
		return 0;
	}
}
