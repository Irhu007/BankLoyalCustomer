/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bankloyalcustomer;

import java.util.HashMap;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class BankMapper extends Mapper<LongWritable, Text, Text, Text>
{
   private HashMap<String, String> person = new HashMap<>();      // [ [ {MGR: 2} {DLP:5} {HR:6} ] ]
                                                                                                                                 
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
	/* read data from distributed cache */
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.getLocal(conf);
        Path[] dataFile = DistributedCache.getLocalCacheFiles(conf);
        
	String record = "";        

		    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(dataFile[0])));
		    record = br.readLine();          
		    while (record != null) 
		    {
			String data[] = record.split(",");                 
			
			person.put(data[0].trim(), data[1].trim());     //{{person ID},{Name}}
			record = br.readLine();
		    }
    }    
    
     @Override
    public void map(LongWritable key, Text value, Context con)throws IOException, InterruptedException
    {
        String[] fields= value.toString().split(",");
        
        String name = person.get(fields[0]);
        
        con.write(new Text(fields[0]), new Text(name+","+fields[3]+","+fields[4]+","+fields[5]));
    }
}