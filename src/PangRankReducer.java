import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PangRankReducer extends Reducer<Text, Text, NullWritable, Text> {
	@Override
	// <nodeName, graphNode> + <nodeName, pageRank>
	// <null, nodeName + "\t" + pageRank + "\t" + links>
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double pageRank = 0.0;
		final double DAMPING_FACTOR = 0.85;
		String graphNode = "";

		for (Text value : values) {
			String str = value.toString();

			if (str.startsWith("P#")) { // Value is pageRank.
				pageRank += Double.parseDouble(str.substring(2));
			} else if (str.startsWith("G#")) { // Value is graph structure.
				if (str.length() == "G#".length()) {
					graphNode = "";
				} else {
					graphNode = str.substring(2);
				}
			}
		}

		pageRank = (1 - DAMPING_FACTOR) + (DAMPING_FACTOR * pageRank);

		if ("".equals(graphNode)) {
			graphNode = key.toString() + "\t" + pageRank;
		} else {
			graphNode = key.toString() + "\t" + pageRank + "\t" + graphNode;
		}

		context.write(NullWritable.get(), new Text(graphNode));
	}
}