import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Wei on 9/17/14.
 */
public class PageRankCombiner extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double pageRank = 0.0;
		final double DAMPING_FACTOR = 0.85;
		String graphNode = "";

		for (Text value : values) {
			String str = value.toString();

			if (str.startsWith("P#")) { // Value is pageRank.
				pageRank += Double.parseDouble(str.substring(2));
			} else if (str.startsWith("G#")) { // Value is graph structure.
				context.write(key, new Text(str));
			}
		}

		context.write(key, new Text("P#" + pageRank));
	}
}
