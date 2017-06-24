package com.laboros.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	protected void setup(Context context) throws java.io.IOException,
			InterruptedException {
	};

	@Override
	protected void map(LongWritable arg0, Text arg1, Context context)
			throws java.io.IOException, InterruptedException {
	};

	protected void cleanup(Context context) throws java.io.IOException,
			InterruptedException {
	};

}
