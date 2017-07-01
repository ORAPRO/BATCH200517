package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException 
			{
		//key -- 0
		//value -- DEER RIVER RIVER
		
//		final long offset=key.get();
		final String inputLine=value.toString();
		final String wordSeperator=" "; //Need to move to properties
		if(StringUtils.isNotEmpty(inputLine))
		{
			final String[] words=StringUtils.splitPreserveAllTokens(inputLine, wordSeperator);
			for (int i = 0; i < words.length; i++) 
			{
				context.write(new Text(words[i]), new IntWritable(1));
			}
		}
		
	};
}
