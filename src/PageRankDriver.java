import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.text.DecimalFormat;

/**
 * Created by Wei on 9/15/14.
 */
public class PageRankDriver {
	public static void main(String[] args) throws Exception {
		if (args.length != 6) {
			System.err.println("Usage: PageRankJob <input path> <output path> " +
					"<input fileNUm> <nodeNum> <mode 1, 2, 0> <compressnode >");
			System.exit(-1);
		}
		boolean result = false;
		final int PAGERANK_ITER_NUM = 10;
		int inputFileNum = Integer.parseInt(args[2]);
		int nodeNum = Integer.parseInt(args[3]);
		int mode = Integer.parseInt(args[4]);
		int compressMode = Integer.parseInt(args[5]);

		Configuration conf = new Configuration();


		// Data processing job
		if (mode == 0 || mode == 1) {
			Job job = new Job(conf);
			job.setJarByClass(PageRankDriver.class);
			job.setJobName("Data Processing");

			job.setInputFormatClass(SequenceFileInputFormat.class);

			// IdentityReducer Performs no reduction, writing all input values directly to the output.
			job.setNumReduceTasks(0);

			job.setMapperClass(DataProcessMapper.class);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			for (int i = 0; i <= inputFileNum; i++) {
				DecimalFormat df = new DecimalFormat("00000");

				FileInputFormat.addInputPath(job, new Path(args[0] + "/metadata-" + df.format(i)));
			}
			FileOutputFormat.setOutputPath(job, new Path(args[1] + "-0"));

			if (compressMode == 0) {
				job.setOutputFormatClass(TextOutputFormat.class);
			} else if (compressMode == 3) {
				job.setOutputFormatClass(SequenceFileOutputFormat.class);
				SequenceFileOutputFormat.setCompressOutput(job, true);
				SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
				SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
			} else if (compressMode == 4){
				job.setOutputFormatClass(SequenceFileOutputFormat.class);
			}

			if (!job.waitForCompletion(true)) {
				System.err.println("Data processing error.");
			} else {
				System.out.println("Data processing finish.");
			}
		}

		if (mode == 0 || mode == 2) {
			int iter = 1;

			while (iter <= PAGERANK_ITER_NUM) {
				conf = new Configuration();
				//conf.setBoolean("mapred.compress.map.output", true);
				//conf.setClass("mapred.map.output.compression.codec",
								//SnappyCodec.class, CompressionCodec.class);

				System.out.println("Page rank iteration + " + iter + ".");
				Job jobIter = new Job(conf);
				jobIter.setJarByClass(PageRankDriver.class);
				jobIter.setJobName("PageRank iteration " + iter);

				jobIter.setMapperClass(PageRankMapper.class);
				jobIter.setMapOutputKeyClass(Text.class);
				jobIter.setMapOutputValueClass(Text.class);

				jobIter.setCombinerClass(PageRankCombiner.class);
				jobIter.setNumReduceTasks(nodeNum * 2);

				// jobIter.setNumReduceTasks(3*10);
				jobIter.setReducerClass(PangRankReducer.class);
				jobIter.setOutputKeyClass(NullWritable.class);
				jobIter.setOutputValueClass(Text.class);

				jobIter.setInputFormatClass(TextInputFormat.class);
				jobIter.setOutputFormatClass(TextOutputFormat.class);


				//jobIter.setInputFormatClass(SequenceFileInputFormat.class);
				//jobIter.setOutputFormatClass(SequenceFileOutputFormat.class);

				FileInputFormat.addInputPath(jobIter, new Path(args[1] + "-" + (iter - 1)));
				FileOutputFormat.setOutputPath(jobIter, new Path(args[1] + "-" + iter));
				//SequenceFileOutputFormat.setCompressOutput(jobIter, true);
				//SequenceFileOutputFormat.setOutputCompressionType(jobIter, SequenceFile.CompressionType.BLOCK);
				//SequenceFileOutputFormat.setOutputCompressorClass(jobIter, SnappyCodec.class);

				if (!jobIter.waitForCompletion(true)) {
					System.err.println("PageRank iteration " + iter + " error.");
				}

				iter++;
			}
		}
	}
}
