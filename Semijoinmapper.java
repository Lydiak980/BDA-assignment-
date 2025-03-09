import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;

public class SemiJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    private HashSet<String> keySet = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException {
        // Load the small dataset (S) from the Distributed Cache
        Path[] cacheFiles = context.getLocalCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            try (BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    keySet.add(line.trim());  // Store keys in HashSet
                }
            }
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String recordKey = fields[0]; // Assuming first field is the key
        
        if (keySet.contains(recordKey)) {
            context.write(new Text(recordKey), value); // Emit only if key exists in HashSet
        }
    }
}
