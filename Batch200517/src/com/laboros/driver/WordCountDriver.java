package com.laboros.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
		return 0;
	}
}
