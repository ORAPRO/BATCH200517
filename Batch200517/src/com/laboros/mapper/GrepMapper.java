package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GrepMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	String search_str = null;

	protected void setup(Context context) throws java.io.IOException,
			InterruptedException {
		Configuration conf=context.getConfiguration();
		search_str=conf.get("SEARCH_STR");
	};

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

		// key -- 0
		// value -- DEER RIVER RIVER

		// final long offset=key.get();
		final String inputLine = value.toString();
		final String wordSeperator = " "; // Need to move to properties
		if (StringUtils.isNotEmpty(inputLine)) {
			final String[] words = StringUtils.splitPreserveAllTokens(
					inputLine, wordSeperator);
			for (String word : words) {
				if (StringUtils.equalsIgnoreCase(word, search_str)) {
					context.write(new Text(word), new IntWritable(1));
				}
			}
		}

	};
}
