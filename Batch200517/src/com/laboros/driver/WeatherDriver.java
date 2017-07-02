package com.laboros.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.WeatherMapper;
import com.laboros.reducer.WeatherReducer;

public class WeatherDriver extends Configured implements Tool {

	public static void main(String[] args) {
		
		System.out.println("MAIN METHOD");
		//Step: 1 Validations input and output provided
		if(args.length<2)
		{
			System.out.println("JAVA USAGE "+WeatherDriver.class.getName()+" /hdfs/input/file /path/to/hdfs/destination/directory");
			return;
		}
		
		//Step: 2 Load Configuration
		
		Configuration conf = new Configuration(Boolean.TRUE);
		
		try {
			int i= ToolRunner.run(conf, new WeatherDriver(), args);
			
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
		Job weatherDriver =Job.getInstance(conf, WeatherDriver.class.getName());
		
		//step-3 : setting the mapper classpath for the client.jar
		weatherDriver.setJarByClass(WeatherDriver.class);
		//step-4 : Setting input
		final String hdfsInput = args[0];
		final Path hdfsInputPath = new Path(hdfsInput);
		TextInputFormat.addInputPath(weatherDriver, hdfsInputPath);
		weatherDriver.setInputFormatClass(TextInputFormat.class);
		
		//step-5: Setting output
		final String hdfsOuputDir = args[1];
		final Path hdfsOuputDirPath = new Path(hdfsOuputDir);
		TextOutputFormat.setOutputPath(weatherDriver, hdfsOuputDirPath);
		weatherDriver.setOutputFormatClass(TextOutputFormat.class);
		//step-6: Setting mapper
		weatherDriver.setMapperClass(WeatherMapper.class);
		//step-7: Setting mapper output key and value classes
		
//		weatherDriver.setMapOutputKeyClass(Text.class);
//		weatherDriver.setMapOutputValueClass(Text.class);
		//step-8: setting reducer
		weatherDriver.setReducerClass(WeatherReducer.class);
		//step-9: Setting reducer output key and value classes
		
		weatherDriver.setOutputKeyClass(Text.class);
		weatherDriver.setOutputValueClass(Text.class);
		//step: trigger method
		weatherDriver.waitForCompletion(Boolean.TRUE);
		return 0;
	}
}
