package com.laboros.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.CustomerMapper;
import com.laboros.mapper.TxnMapper;
import com.laboros.reducer.ReducerJoinReducer;

public class ReducerJoinDriver extends Configured implements Tool {

	public static void main(String[] args) {

		System.out.println("MAIN METHOD");
		// Step: 1 Validations input and output provided
		if (args.length < 3) {
			System.out
					.println("JAVA USAGE "
							+ ReducerJoinDriver.class.getName()
							+ " /hdfs/input/custs/file /hdfs/input/txns/file /path/to/hdfs/destination/directory");
			return;
		}

		// Step: 2 Load Configuration

		Configuration conf = new Configuration(Boolean.TRUE);

		try {
			int i = ToolRunner.run(conf, new ReducerJoinDriver(), args);

			if (i == 0) {
				System.out.println("SUCCESS");
			} else {
				System.out.println("FAILURE");
			}
		} catch (Exception e) {
			System.out.println("FAILURE");
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		// Steps =10
		// Step-1 : Getting the configuration
		Configuration conf = super.getConf();

		// step-2: Creating the job instance
		Job reducerJoinDriver = Job.getInstance(conf,
				ReducerJoinDriver.class.getName());

		// step-3 : setting the mapper classpath for the client.jar
		reducerJoinDriver.setJarByClass(ReducerJoinDriver.class);

		// step-4 setting customer and Transaction input

		// step-4a: Setting the customer input
		final String hdfsCustInput = args[0];
		final Path hdfsCustInputPath = new Path(hdfsCustInput);

		MultipleInputs.addInputPath(reducerJoinDriver, hdfsCustInputPath,
				TextInputFormat.class, CustomerMapper.class);

		// step-4b: Setting the Transaction input
		final String hdfsTxnInput = args[1];
		final Path hdfsTxnInputPath = new Path(hdfsTxnInput);

		MultipleInputs.addInputPath(reducerJoinDriver, hdfsTxnInputPath,
				TextInputFormat.class, TxnMapper.class);


		// step-5: Setting output
		final String hdfsOuputDir = args[2];
		final Path hdfsOuputDirPath = new Path(hdfsOuputDir);
		TextOutputFormat.setOutputPath(reducerJoinDriver, hdfsOuputDirPath);
		reducerJoinDriver.setOutputFormatClass(TextOutputFormat.class);
		

		//step: 6  Setting MapOutput Key and value calsses 
		reducerJoinDriver.setMapOutputKeyClass(LongWritable.class);
		reducerJoinDriver.setMapOutputValueClass(Text.class);
		
		// step-8: setting reducer
		reducerJoinDriver.setReducerClass(ReducerJoinReducer.class);
		// step-9: Setting reducer output key and value classes
		
		
		reducerJoinDriver.setOutputKeyClass(Text.class);
		reducerJoinDriver.setOutputValueClass(Text.class);
		// step: trigger method
		reducerJoinDriver.waitForCompletion(Boolean.TRUE);
		return 0;
	}
}
