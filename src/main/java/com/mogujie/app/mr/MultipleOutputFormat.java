package com.mogujie.app.mr;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 
 * @author beifeng
 * @Date 2013-8-19 ä¸????5:32:11
 * 
 */
public abstract class MultipleOutputFormat<K extends WritableComparable<?>, V extends Writable>
		extends FileOutputFormat<K, V> {

	// ??¥å?£ç±»ï¼????è¦???¨è????¨ç??åº?ä¸?å®????generateFileNameForKeyValue??¥è?·å?????ä»¶å??
	private MultiRecordWriter writer = null;

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		if (writer == null) {
			writer = new MultiRecordWriter(job, getTaskOutputPath(job));
		}
		return writer;
	}

	/**
	 * get task output path
	 * 
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	private Path getTaskOutputPath(TaskAttemptContext conf) throws IOException {
		Path workPath = null;
		OutputCommitter committer = super.getOutputCommitter(conf);
		if (committer instanceof FileOutputCommitter) {
			workPath = ((FileOutputCommitter) committer).getWorkPath();
		} else {
			Path outputPath = super.getOutputPath(conf);
			if (outputPath == null) {
				throw new IOException("Undefined job output-path");
			}
			workPath = outputPath;
		}
		return workPath;
	}

	/**
	 * ???è¿?key, value, conf??¥ç¡®å®?è¾???ºæ??ä»¶å??ï¼??????©å?????ï¼? Generate the file output file name based
	 * on the given key and the leaf file name. The default behavior is that the
	 * file name does not depend on the key.
	 * 
	 * @param key
	 *            the key of the output data
	 * @param name
	 *            the leaf file name
	 * @param conf
	 *            the configure object
	 * @return generated file name
	 */
	protected abstract String generateFileNameForKeyValue(K key, V value,
			Configuration conf);

	/**
	 * å®???°è?°å???????¥å??RecordWriterç±? ï¼??????¨ç±»ï¼?
	 * 
	 * @author zhoulongliu
	 * 
	 */
	public class MultiRecordWriter extends RecordWriter<K, V> {
		/** RecordWriter???ç¼?å­? */
		private HashMap<String, RecordWriter<K, V>> recordWriters = null;
		private TaskAttemptContext job = null;
		/** è¾???ºç??å½? */
		private Path workPath = null;

		public MultiRecordWriter(TaskAttemptContext job, Path workPath) {
			super();
			this.job = job;
			this.workPath = workPath;
			recordWriters = new HashMap<String, RecordWriter<K, V>>();
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			Iterator<RecordWriter<K, V>> values = this.recordWriters.values()
					.iterator();
			while (values.hasNext()) {
				values.next().close(context);
			}
			this.recordWriters.clear();
		}

		@Override
		public void write(K key, V value) throws IOException,
				InterruptedException {
			// å¾???°è????ºæ??ä»¶å??
			String baseName = generateFileNameForKeyValue(key, value,
					job.getConfiguration());
			// å¦????recordWriters???æ²¡æ?????ä»¶å??ï¼???£ä??å°±å»ºç«??????????å°±ç?´æ?¥å????¼ã??
			RecordWriter<K, V> rw = this.recordWriters.get(baseName);
			if (rw == null) {
				rw = getBaseRecordWriter(job, baseName);
				this.recordWriters.put(baseName, rw);
			}
			rw.write(key, value);
		}

		// ${mapred.out.dir}/_temporary/_${taskid}/${nameWithExtension}
		private RecordWriter<K, V> getBaseRecordWriter(TaskAttemptContext job,
				String baseName) throws IOException, InterruptedException {
			Configuration conf = job.getConfiguration();
			// ??¥ç????????ä½¿ç?¨è§£??????
			boolean isCompressed = getCompressOutput(job);
			String keyValueSeparator = AccessLogMapperTest.COLUMN_SPLIT;
			//String keyValueSeparator = ",";
			RecordWriter<K, V> recordWriter = null;
			if (isCompressed) {
				Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
						job, GzipCodec.class);
				CompressionCodec codec = ReflectionUtils.newInstance(
						codecClass, conf);
				Path file = new Path(workPath, baseName
						+ codec.getDefaultExtension());
				FSDataOutputStream fileOut = file.getFileSystem(conf).create(
						file, false);
				// è¿???????ä½¿ç?¨ç?????å®?ä¹????OutputFormat
				recordWriter = new LineRecordWriter<K, V>(new DataOutputStream(
						codec.createOutputStream(fileOut)), keyValueSeparator);
			} else {
				Path file = new Path(workPath, baseName);
				FSDataOutputStream fileOut = file.getFileSystem(conf).create(
						file, false);
				// è¿???????ä½¿ç?¨ç?????å®?ä¹????OutputFormat
				recordWriter = new LineRecordWriter<K, V>(fileOut,
						keyValueSeparator);
			}
			return recordWriter;
		}
	}

}
