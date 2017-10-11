import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TpHadoop2 {

	public static class TpHadoop2Mapper extends Mapper<Object, Text, IntWritable, Text> {

		private Text newValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			int colonne = 0;
			String[] result = value.toString().split(",");
			while (colonne<result.length) {
				newValue.set(key.toString() + ";" + result[colonne]);
				context.write(new IntWritable(colonne), newValue);
				colonne++;
			}
		}
	}

	public static class TpHadoop2Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		private Text result = new Text();

		public void reduce(IntWritable key, Iterable<Text> values, Context context)	throws IOException, InterruptedException {

			Map<Integer, String> listValues = new TreeMap<Integer, String>();
			String valeurString = "";

			for(Text value : values) {
				listValues.put(
						Integer.parseInt(value.toString().split(";")[0]),
						value.toString().split(";")[1]);
			}
			
			for(Map.Entry<Integer, String> entry : listValues.entrySet()) {
				valeurString += entry.getValue() + ",";
			}
			valeurString = valeurString.substring(0, valeurString.length()-1);

			result.set(valeurString);

			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TpHadoop2");
		job.setJarByClass(TpHadoop2.class);

		job.setMapperClass(TpHadoop2Mapper.class);
		job.setReducerClass(TpHadoop2Reducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}