package com.laboros.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.DeIdentifyMapper;

public class DeIdentifyDriver extends Configured implements Tool {

	public static void main(String[] args) {
		
		System.out.println("MAIN METHOD");
		//Step: 1 Validations input and output provided
		if(args.length<2)
		{
			System.out.println("JAVA USAGE "+DeIdentifyDriver.class.getName()+" /hdfs/input/file /path/to/hdfs/destination/directory");
			return;
		}
		
		//Step: 2 Load Configuration
		
		Configuration conf = new Configuration(Boolean.TRUE);
		
		try {
			int i= ToolRunner.run(conf, new DeIdentifyDriver(), args);
			
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
		Job deIdentifyDriver =Job.getInstance(conf, DeIdentifyDriver.class.getName());
		
		//step-3 : setting the mapper classpath for the client.jar
		deIdentifyDriver.setJarByClass(DeIdentifyDriver.class);
		//step-4 : Setting input
		final String hdfsInput = args[0];
		final Path hdfsInputPath = new Path(hdfsInput);
		TextInputFormat.addInputPath(deIdentifyDriver, hdfsInputPath);
		deIdentifyDriver.setInputFormatClass(TextInputFormat.class);
		
		//step-5: Setting output
		final String hdfsOuputDir = args[1];
		final Path hdfsOuputDirPath = new Path(hdfsOuputDir);
		TextOutputFormat.setOutputPath(deIdentifyDriver, hdfsOuputDirPath);
		deIdentifyDriver.setOutputFormatClass(TextOutputFormat.class);
		//step-6: Setting mapper
		deIdentifyDriver.setMapperClass(DeIdentifyMapper.class);
		//step-7: Setting mapper output key and value classes
		deIdentifyDriver.setMapOutputKeyClass(Text.class);
		deIdentifyDriver.setMapOutputValueClass(NullWritable.class);
		
		deIdentifyDriver.setNumReduceTasks(0);
		//step-8: setting reducer
//		deIdentifyDriver.setReducerClass(WordCountReducer.class);
		//step-9: Setting reducer output key and value classes
//		deIdentifyDriver.setOutputKeyClass(Text.class);
//		deIdentifyDriver.setOutputValueClass(IntWritable.class);
		//step: trigger method
		deIdentifyDriver.waitForCompletion(Boolean.TRUE);
		return 0;
	}
}
