package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomerMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException 
	{
		//key -- 0
		//value -- 4000001,Kristina,Chung,55,Pilot
		
		final String iLine = value.toString();
		
		//avoid null pointer
		final String DATA_SEPERATOR=",";// Move to properties 
		if(StringUtils.isNotEmpty(iLine))
		{
			final String[] columns = StringUtils.splitPreserveAllTokens(iLine, DATA_SEPERATOR);
			if(StringUtils.isNotEmpty(columns[0]))
			{
			context.write(new LongWritable(Long.parseLong(columns[0])), 
					new Text("CUSTS\t"+columns[1]));
			}
		}
	};
}
