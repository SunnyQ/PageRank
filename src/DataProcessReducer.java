import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Wei on 9/21/14.
 */
public class DataProcessReducer extends Reducer<Text, Text, NullWritable, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Set<String> set = new HashSet<String>();
		String nodeName = key.toString();

		// Add and remove duplicated links.
		for (Text value : values) {
			String str = value.toString();

			if (str != null && str.length() > 0) {
				String[] links = str.split("\t");

				for (String link : links) {
					set.add(link);
				}
			}
		}

		String res = "";

		for (String link : set) {
			res += "\t" + link;
		}

		if (res.length() > 0) res = res.substring(1);

		context.write(NullWritable.get(), new Text(nodeName + "\t1.0\t" + res));
	}
}
