package com.subhadip.datory.preprocessing.core.formats.delimited;

import com.subhadip.datory.preprocessing.core.Application;
import com.subhadip.datory.preprocessing.core.shared.Parameters;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants._NEW_LINE_;
import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants._SPACE_;
import static com.subhadip.datory.preprocessing.core.formats.delimited.DelimitedFileAttributes.*;
import static java.lang.Long.max;

public class DelimitedLineRecordReader extends RecordReader<LongWritable, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    private long start = 0L,
                   end = 0L,
                   pos = 0L;

    private LineReader reader = null;
    private LongWritable key = new LongWritable();
    private Text value = new Text();



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
                    (buffer.charAt(buffer.getLength() - 1) == delimitedRecordEndChar) && //[RECORD_END_CHAR]
                    (buffer.charAt(buffer.getLength() + 1) == delimitedRecordColumnDelimiterChar) && // [COLUMN_DELMITER]
                    (pos <= end)) {
                readAnotherLine = true;
            }
            else {
                readAnotherLine = false;
            } // seek head hasn't passed the split

            //TODO: Check when the is no RECORD_END_CHAR for first split.

        }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(key == null) key = new LongWritable();

        key.set(pos);

        if(value == null) value = new Text();

        int currentSize = 0;
        int currentFields = 0;
        Text tempStore = new Text();
        boolean firstRead = true;

        while(currentFields < delimitedExpectedColsNum) {
            while(pos < end) {
                currentSize = reader.readLine(tempStore, delimitedMaxLineLength,
                        Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), delimitedMaxLineLength));

                if(currentSize == 0) break;
                pos += currentSize;

                if(currentSize < delimitedMaxLineLength) break;
                LOG.info("Skipped line of size " + currentSize + " at pos " + (pos - currentSize));
            }

            if(currentSize == 0) break;
            else {
                String record = tempStore.toString();
                StringTokenizer fields = new StringTokenizer(record,Character.toString(delimitedRecordColumnDelimiterChar));

                currentFields += fields.countTokens();
                if(firstRead) {
                    value = new Text();
                    firstRead = false;
                }

                if(currentFields != delimitedExpectedColsNum)
                    value.append(tempStore.getBytes(), 0, tempStore.getLength());
                else value.append(tempStore.getBytes(), 0, tempStore.getLength());
            }
        }

        if(currentSize == 0) {
            key = null; value = null;
            return false;
        }
        else return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        System.out.print(value.toString());
        System.out.println();


        return replaceControlCharacters(value.toString());
    }

    @Override
    public float getProgress() throws IOException, InterruptedException{
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float)(end - start ));
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null){
            reader.close();
        }
    }

    // Replace Control Characters
    private Text replaceControlCharacters(String str){
        // remove all newline first, and add a new line at end.
        str = str.replaceAll(_NEW_LINE_, _SPACE_) + _NEW_LINE_;
        for ( Map.Entry<String, String> entry : Parameters.controlCharacterReplacementMap.entrySet())
            str = Pattern.compile(entry.getKey()).matcher(str).replaceAll(entry.getValue());

        return new Text(str);
    }
}
