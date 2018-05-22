package sk.stopangin.mapper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class VehicleCount {

	public static void main(String[] args) throws Exception {


		String inputPath="/user/datarepl/us-vehicle-fuel-economy-data-1984-2017.csv";
		String outputPath="/user/datarepl/processed.csv";
		//Define MapReduce job
		Job job = new Job();
		job.setJarByClass(VehicleCount.class);
		job.setJobName("VehicleCount");

		//Set input and output locations
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		//Set Input and Output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//Set Mapper and Reduce classes
		job.setMapperClass(VehicleCountMapper.class);
		job.setReducerClass(VehicleCountReducer.class);


		//Output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//Submit job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
