
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.*;

public class TeraGen {

	/**
	 * An input format that assigns ranges of longs to each mapper.
	 */
	static class RandomGenerator {
		private long seed = 0;
		private static final long mask32 = (1l<<32) - 1;
		/**
		 * The number of iterations separating the precomputed seeds.
		 */
		private static final int seedSkip = 128 * 1024 * 1024;
		/**
		 * The precomputed seed values after every seedSkip iterations.
		 * There should be enough values so that a 2**32 iterations are 
		 * covered.
		 */
		private static final long[] seeds = new long[]{0L,
			4160749568L,
			4026531840L,
			3892314112L,
			3758096384L,
			3623878656L,
			3489660928L,
			3355443200L,
			3221225472L,
			3087007744L,
			2952790016L,
			2818572288L,
			2684354560L,
			2550136832L,
			2415919104L,
			2281701376L,
			2147483648L,
			2013265920L,
			1879048192L,
			1744830464L,
			1610612736L,
			1476395008L,
			1342177280L,
			1207959552L,
			1073741824L,
			939524096L,
			805306368L,
			671088640L,
			536870912L,
			402653184L,
			268435456L,
			134217728L,
		};

		/**
		 * Start the random number generator on the given iteration.
		 * @param initalIteration the iteration number to start on
		 */
		RandomGenerator(long initalIteration) {
			int baseIndex = (int) ((initalIteration & mask32) / seedSkip);
			seed = seeds[baseIndex];
			for(int i=0; i < initalIteration % seedSkip; ++i) {
				next();
			}
		}

		RandomGenerator() {
			this(0);
		}

		long next() {
			seed = (seed * 3141592621l + 663896637) & mask32;
			return seed;
		}
	}

	static class RangeInputFormat 
	extends InputFormat<LongWritable, NullWritable> {
		public  List<InputSplit>  getSplits(JobContext job) throws IOException,
		InterruptedException{
			long totalRows = getNumberOfRows(job.getConfiguration());
			int numSplits=job.getConfiguration().getInt("mapred.max.split.size",2);
			long rowsPerSplit = totalRows / numSplits;
			System.out.println("Generating " + totalRows + " using " + numSplits + 
					" maps with step of " + rowsPerSplit);

			List<InputSplit>  splits = new ArrayList<InputSplit>();
			long currentRow = 0;
			for(int split=0; split < numSplits-1; ++split) {
				splits.add( new RangeInputSplit(currentRow, rowsPerSplit));
				currentRow += rowsPerSplit;
			}
			splits.add(new RangeInputSplit(currentRow, 
					totalRows - currentRow));
			return splits;
		}

		static class RangeInputSplit extends InputSplit  implements Writable{
			long firstRow;
			long rowCount;

			public RangeInputSplit() { }

			public RangeInputSplit(long offset, long length) {
				firstRow = offset;
				rowCount = length;
			}

			public long getLength() throws IOException {
				return 0;
			}

			public String[] getLocations() throws IOException {
				return new String[]{};
			}

			public void readFields(DataInput in) throws IOException {
				firstRow = WritableUtils.readVLong(in);
				rowCount = WritableUtils.readVLong(in);
			}

			public void write(DataOutput out) throws IOException {
				WritableUtils.writeVLong(out, firstRow);
				WritableUtils.writeVLong(out, rowCount);
			}
		}

	
		static class RangeRecordReader 
		extends RecordReader<LongWritable, NullWritable> {
			long startRow;
			long finishedRows;
			long totalRows;
			LongWritable key;
public RangeRecordReader() {}
			public RangeRecordReader(RangeInputSplit split) {
				startRow = split.firstRow;
				finishedRows = 0;
				totalRows = split.rowCount;
			}

			public void close() throws IOException {
				// NOTHING
			}

			@Override
			public LongWritable getCurrentKey() throws IOException,InterruptedException {
				return key;
			}

			@Override
			public NullWritable getCurrentValue() throws IOException, InterruptedException {
				return NullWritable.get();

			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return finishedRows / (float) totalRows;
			}

			public long getPos() throws IOException {
				return finishedRows;
			}


			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				if (key == null) {
					key = new LongWritable();
				}
				if (finishedRows < totalRows) {
					key.set(startRow + finishedRows);
					finishedRows += 1;
					return true;
				} else {
					return false;
				}
			}
		
			@Override
			public void initialize(InputSplit split, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
				startRow = ((RangeInputSplit) split).firstRow;
				finishedRows = 0;
				totalRows = ((RangeInputSplit) split).rowCount;


			}
		}

		@Override
		public RecordReader<LongWritable, NullWritable> createRecordReader(
				InputSplit split, TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return new RangeRecordReader((RangeInputSplit) split);
			
		}
		
	}
	public  static class SortGenMapper 
	extends Mapper<LongWritable, NullWritable, Text, Text> {
	public SortGenMapper(){}
		private Text key = new Text();
		private Text value = new Text();
		private RandomGenerator rand;
		int d=0;
		public void setup (Context context) {
			//Get the value of n from the job configuration
			d = getDim(context.getConfiguration());
		}
		private void addDim(long rowId) {
			byte[] rowid = Integer.toString((int) rowId).getBytes();
			value.append(rowid,0, Math.min(rowid.length, 10));
		}
		private void Getkey(long rowId) {
			key.clear();
			byte[] rowid = Integer.toString((int) rowId).getBytes();
			key.append(rowid, 0, Math.min(rowid.length, 10));
		}

		public void map(LongWritable row, NullWritable ignored,
				Context context) throws IOException {
			long rowId = row.get();
			if (rand == null) {      
				rand = new RandomGenerator(rowId);
			}
			Getkey(rowId);
			value.clear();
			byte []buf=new byte[3];
			buf[0]='(';	
			buf[1]=',';
			buf[2]=')';
			value.append(buf,0, 1);
			for(int i=0;i<d;i++){
				long x=rand.next();
				addDim(x%1000);
				if(i<d-1)value.append(buf,1, 1);
			}

			value.append(buf,2, 1);
			try {
				context.write(key, value);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	static long getNumberOfRows(Configuration conf) {
		return conf.getLong("terasort.num-rows", 0);
	}
	static int getDim(Configuration conf) {
		return conf.getInt("terasort.d", 2);
	}

	static void setNumberOfRows(Configuration conf, long numRows) {
		conf.setLong("terasort.num-rows", numRows);
	}
	static void setDim(Configuration conf, int d) {
		conf.setInt("terasort.d", d);
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: TeraGen <n>  <out> <d>");
			System.exit(2);
		}
		Configuration conf = new Configuration(); 

		setNumberOfRows(conf, Long.parseLong(args[0]));
		setDim(conf, Integer.parseInt(args[2]));
		Job job = new Job(conf, "TeraGen");  
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJarByClass(TeraGen.class);

		job.setMapperClass(SortGenMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(RangeInputFormat.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

