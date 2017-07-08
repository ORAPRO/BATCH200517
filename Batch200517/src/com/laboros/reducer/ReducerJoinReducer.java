package com.laboros.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJoinReducer extends Reducer<LongWritable, Text, Text, Text> {

	@Override
	protected void reduce(
			LongWritable key,
			java.lang.Iterable<Text> values,
			Context context)
			throws java.io.IOException, InterruptedException {

		// key -- 4000001
		// values -- {CUSTS\tkristian,
		// TXNS\t403.5,TXNS\t56.4,TXNS\t545.3,TXNS\t345.6,TXNS\t364.5 }

		String name = null;

		int numTxns = 0;
		float totalAmount = 0;

		for (Text input : values) {

			final String tokens[] = StringUtils.splitPreserveAllTokens(
					input.toString(), "\t");
			// tokens[0]=CUSTS/TXNS
			if (StringUtils.equalsIgnoreCase(tokens[0], "TXNS")) {
				numTxns++;
				totalAmount += Float.parseFloat(tokens[1]);
			} else {
				name = tokens[1];
			}
		}

		if(StringUtils.isNotEmpty(name))
		{
		context.write(new Text(name), new Text(numTxns + "\t" + totalAmount));
		}
	};

}
