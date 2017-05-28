package com.laboros;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//java -cp HDFSService.jar com.laboros.HDFSService WordCount.txt /user/edureka

public class HDFSService extends Configured implements Tool {

	
	public static void main(String[] args) 
	{
		//Step: 1 Validations input and output provided
		if(args.length!=2)
		{
			System.out.println("JAVA USAGE HDFSService /path/to/edgenode/local/file /path/to/hdfs/destination/directory");
			return;
		}
		
		//Step: 2 Load Configuration
		
		Configuration conf = new Configuration(Boolean.TRUE);
		
		conf.set("fs.defaultFS", "hdfs://localhost:8020");
		
		try {
			int i= ToolRunner.run(conf, new HDFSService(), args);
			
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
