package com.laboros.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, java.lang.Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException {
		// key -- 2016
		// value -- {20160101\t33.6, 20160102\t-23.5,.....}

		String finalDate = null;
		float max_temp = Float.MIN_VALUE;

		for (Text tabSepValues : values) 
		{
			
			final String tabValues=tabSepValues.toString();
			String[] tempValues = StringUtils.splitPreserveAllTokens(
					tabValues, "\t");
			
			float currentTemp=Float.parseFloat(tempValues[0]);
			if(max_temp< currentTemp)
			{
				max_temp=currentTemp;
				finalDate=tempValues[1];
			}
		}

		context.write(key, new Text(finalDate + "\t" + max_temp));
	};
}
