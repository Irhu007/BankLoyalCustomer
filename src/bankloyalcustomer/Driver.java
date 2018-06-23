/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bankloyalcustomer;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;
import java.net.URISyntaxException;
//import java.net.URISyntaxException;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException, FileNotFoundException {
        // creating object of configuration class 
	Configuration conf = new Configuration();
        DistributedCache.addCacheFile(new URI("hdfs://localhost:54310/user/hduser/person"), conf);
	
        
        String[] In_Out_files = new GenericOptionsParser(conf, args).getRemainingArgs();
	
	Path input_path = new Path(In_Out_files[0]);
	Path output_directory = new Path(In_Out_files[1]);
        

        //DistributedCache.addCacheFile(new URI("hdfs://localhost:54310/user/hduser/designation.txt"), conf);		
	Job job_obj = new Job(conf, "Bank Loyal Customer"   );
	// setting name of main class
       // job_obj.addCacheFile(new Path("hdfs://localhost:54310/user/hduser/des.txt").toUri());
	job_obj.setJarByClass(Driver.class);
	//name of mapper class
	job_obj.setMapperClass(BankMapper.class);
		// name of reducer class

	job_obj.setReducerClass(BankReducer.class);
		
	job_obj.setOutputKeyClass(Text.class);
	job_obj.setOutputValueClass(Text.class);
	//Sending the file to each node as cache
        
	FileInputFormat.addInputPath(job_obj, input_path);
		
	FileOutputFormat.setOutputPath(job_obj, output_directory);
		
	output_directory.getFileSystem(job_obj.getConfiguration()).delete(output_directory,true);
	System.exit(job_obj.waitForCompletion(true) ? 0 : 1);

    }
    
}
