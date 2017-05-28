package com.laboros;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//java -cp HDFSService.jar com.laboros.HDFSService WordCount.txt /user/edureka

public class HDFSService extends Configured implements Tool {

	
	public static void main(String[] args) 
	{
		
		System.out.println("MAIN METHOD");
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
	public int run(String[] args) throws Exception 
	{
		System.out.println("IN RUN METHOD");
		
		//step-1 : Get the configuration object created in the main method
		
		Configuration conf = super.getConf();
		//step : 2 Get the HDFS File System object
		
		FileSystem hdfs = FileSystem.get(conf);
		
		//step : 3 -- Create Metadata = Create Empty File + Add Metadata to Namenode
		
		final String inputFile = args[0];// WordCount.txt
		
		final String hdfsDestDir=args[1]; //user/edureka
		
		final Path hdfsDestDirAlongWithFileName = new Path(hdfsDestDir, inputFile); //user/edureka/WordCount.txt
		
		//Get the FSDOS
		FSDataOutputStream fsdos=hdfs.create(hdfsDestDirAlongWithFileName);
		
		//GET THE INPUTSTREAM
		
		InputStream is = new FileInputStream(inputFile);
		
		
		//COPY DATA THROUGH FSDOS + Datastreamer + Handling Failure
		
		IOUtils.copyBytes(is, fsdos, conf, Boolean.TRUE);
		
		return 0;
	}

}
