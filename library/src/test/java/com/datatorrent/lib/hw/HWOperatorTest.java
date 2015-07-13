package com.datatorrent.lib.hw;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;



public class HWOperatorTest {
	
	  @Test
	  public void testing() throws Exception
	  {
		  Configuration conf = new Configuration(false);
//		  conf.set("dt.operator.DummyOutput.attr.PARTITIONER", "com.datatorrent.lib.partitioner.StatelessPartitioner:3");

		  LocalMode lma = LocalMode.newInstance();
//		  lma.prepareDAG(new HWOperator(), conf);
//		  LocalMode.Controller lc = lma.getController();
//		  lc.run(1000);
		  
		  try {
		    lma.prepareDAG(new HWOperator(), conf);
		    LocalMode.Controller lc = lma.getController();
		    lc.run(10);
		  }
		  catch (Exception ex) {
		    throw new RuntimeException(ex);
		  }
	  }

}
