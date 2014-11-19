package com.pzoom.mdsp.mapping;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 * 生成一个新的输出路径，输出文件名包含JOBID
 * @author chenbaoyu
 *
 * @param <K>
 * @param <V>
 */
public class KafkaOutputFormat<K, V> extends TextOutputFormat<K, V> {

    public Path getDefaultWorkFile(TaskAttemptContext context,String extension) throws IOException{
        FileOutputCommitter committer =  (FileOutputCommitter) getOutputCommitter(context);
        JobID jobId = context.getJobID();
        return new Path(committer.getWorkPath(), getUniqueFile(context, "part-" + 
        					jobId.toString().replace("job_", ""), extension));
    }

    public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException{
        Path outDir = getOutputPath(job);
        if (outDir == null) {
            throw new InvalidJobConfException("Output directory not set.");
        }
    }
}	  
