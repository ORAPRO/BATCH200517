package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		// key -- 0
		// value --27516 20160101 2.424 -156.61 71.32 -18.3 -21.8 -20.0 -19.9
		// 0.0 0.00 C -19.2 -24.5 -21.9 83.9 73.7 77.9 -99.000 -99.000 -99.000
		// -99.000 -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0
		
		final String iLine = value.toString();
		if(StringUtils.isNotEmpty(iLine))
		{
			final String iDate=StringUtils.substring(iLine, 6, 14);
			final String year=StringUtils.substring(iDate, 0,4);
			final String max_temp=StringUtils.substring(iLine, 38, 45);
			
			if(StringUtils.isNotEmpty(year))
			{
//				final int iyear = Integer.parseInt(year);
				context.write(new Text(year), new Text(max_temp+"\t"+iDate));
			}
		}

	};
}
