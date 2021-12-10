package com.subhadip.datory.preprocessing.core.formats.delimited;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

import static com.subhadip.datory.preprocessing.core.formats.delimited.DelimitedFileAttributes.*;

import static java.lang.Long.max;

public class DelimitedGenericRecordReader extends RecordReader<LongWritable, Text> {

    private long start = 0L,
                   end = 0L,
                   pos = 0L;

    private LineReader reader = null;
    private LongWritable key = new LongWritable();
    private Text value = new Text();

    private String DELIMITED_RECORD_START_STR = Character.toString(delimitedRecordStartChar);

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        start = max(0L, split.getStart() - 1);
        end = start + split.getLength();

        FSDataInputStream stream = split.getPath().getFileSystem(context.getConfiguration()).open(split.getPath());
        stream.seek(start);

        reader = new LineReader(stream, context.getConfiguration());

        byte firstByte = stream.readByte(); // Cast as int or (char) (firstByte & 0xFF)

        if(firstByte == delimitedRecordStartChar) start = max(0, start - 1);
        stream.seek(start);

        if(start != 0) skipRemainderFromPreviousSplit(reader);
    }


    private void skipRemainderFromPreviousSplit(LineReader reader) throws IOException, InterruptedException {
        boolean readAnotherLine = true;

        while (readAnotherLine) {
            // read next line
            Text buffer = new Text();
            start += reader.readLine(buffer, Integer.MAX_VALUE, Integer.MAX_VALUE); // until next new line
            pos = start;

            if ((buffer.getLength() >= 1) && // something was read
                    (buffer.charAt(buffer.getLength() - 1) == delimitedRecordEndChar) && //[LF]
                    (buffer.charAt(buffer.getLength() + 1) == delimitedRecordColumnDelimiterChar) && // [BELL]
                    (pos <= end)) {
                readAnotherLine = true;
            }
            else {
                readAnotherLine = false;
            } // seek head hasn't passed the split

            //TODO: Check when the is no LF for first split.

        }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        key.set(pos);
        boolean isStartOfRecord = false;
        while (pos < end || isStartOfRecord) {
            // read next line
            Text buffer = new Text();
            pos += reader.readLine(buffer, Integer.MAX_VALUE, Integer.MAX_VALUE);
            // append newly read data to previous data if necessary
            value = isStartOfRecord ? new Text(value + DELIMITED_RECORD_START_STR + buffer) : buffer;
                    // detect if start of record was reached
            isStartOfRecord = buffer.charAt(buffer.getLength() - 1) == delimitedRecordEndChar &&
                    buffer.charAt(buffer.getLength() + 1) == delimitedRecordColumnDelimiterChar;

            // TODO: Handle the last Line, where there is no 'D'??
            // let Spark know that a key-value pair is ready!
            if(!isStartOfRecord)
                return true;
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        System.out.print(value.toString());
        System.out.println();
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException{
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) /(float)(end - start ));
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null){
            reader.close();
        }
    }
}
