/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bankloyalcustomer;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BankReducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text id, Iterable<Text> value, Context con)throws IOException, InterruptedException
	{
            boolean penalty = true;
            int totalDeposit=0;
            String name = "";
            for(Text x:value)
            {
                String[] fields = x.toString().trim().split(",");
                double deposit = Double.parseDouble(fields[1]);
                double withdrawn = Double.parseDouble(fields[2]);
                name = fields[0];
                if(fields[3].equalsIgnoreCase("yes"))
                {
                    penalty = false;
                    break;
                }
                else
                {
                    if(withdrawn>(deposit/2))
                    {
                        penalty = false;
                        break;
                    }
                    else
                    {
                        totalDeposit+=deposit;
                    }
                }
            }
            if(penalty && totalDeposit>10000)
                con.write(id, new Text(name));
        }
}
