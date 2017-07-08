package com.laboros.partitioner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable values, int numPartitions) 
	{
		final String sKey=key.toString();
		if(StringUtils.equalsIgnoreCase(sKey, "hadoop"))
		{
			return 0;
		}
		if(StringUtils.equalsIgnoreCase(sKey, "data"))
		{
			return 1;
		}
		return 2;
	}


}
