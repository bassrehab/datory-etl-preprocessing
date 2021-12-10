package com.subhadip.datory.preprocessing.core.formats.delimited;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class DelimitedInputFormat extends FileInputFormat {

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new DelimitedLineRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }

}
