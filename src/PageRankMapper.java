import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Wei on 9/15/14.
 */
public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	// nodeName [\t] pageRank [\t] link1,link2.link3
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] lineArgs = value.toString().split("\t");
		String nodeName = lineArgs[0];

		double pageRank = Double.parseDouble(lineArgs[1]);
		String graphStr = "";

		if (lineArgs.length > 2) { // Outgoing links exist.
			String[] links = lineArgs[2].split(",@-@,");

			double p = pageRank / links.length;

			// Emit(nid m, PageRank p)
			// Pass PageRank mass to neighbor.
			for (String link : links) {
				context.write(new Text(link), new Text("P#" + p));
			}

			graphStr = lineArgs[2];
		} else {
			graphStr = "";
		}

		// Emit(nid n, Node N)
		// Pass graph structure.
		context.write(new Text(nodeName), new Text("G#" + graphStr));
	}
}
