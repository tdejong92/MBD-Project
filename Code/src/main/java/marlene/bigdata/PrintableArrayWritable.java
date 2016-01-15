package marlene.bigdata;

import org.apache.hadoop.io.*;
import java.util.*;

/**
 * Class that makes it able to print to content of an ArrayWritable in
 * readable fashion. The class is an extension of the ArrayWritable of the
 * Apache Hadoop Framework
 * @author Tanja de Jong & Marlene Hol
 */
public class PrintableArrayWritable extends ArrayWritable {
	
	//Constructor from ArrayWritable is used
	public PrintableArrayWritable() {
		super(IntWritable.class);
	}

	//Constructor from ArrayWritable is used
    public PrintableArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
    }

    //Method that uses the toStrings() method from ArraYWritable
	@Override
    public String[] toStrings() {
    	return super.toStrings();
    }
	
	//Overrides toString() method from ArrayWritable. With this
	//method the content of the ArrayWritable can be read, which
	//makes the result of MapReduce readable. 
    @Override
    public String toString() {
        String[] values = toStrings();
        return values[0] + "\t" + values[1];
    }
}
