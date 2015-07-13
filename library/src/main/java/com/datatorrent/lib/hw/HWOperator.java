package com.datatorrent.lib.hw;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.System.out;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.util.BaseNumberValueOperator;



@SuppressWarnings("unused")
public class HWOperator implements Operator, StreamingApplication {
	public final String HelloWorld = "Hello World";
	
	public final transient DefaultOutputPort<String> sort = new DefaultOutputPort<String>();
	
	
	private static final Logger LOG = LoggerFactory.getLogger(HWOperator.class);

	
	@Override
	public void setup(OperatorContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void teardown() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void beginWindow(long windowId) {
		// TODO Auto-generated method stub
//		System.out.println(HelloWorld);
		LOG.info("Hello WOrld");
		sort.emit(HelloWorld);
	}

	@Override
	public void endWindow() {
		// TODO Auto-generated method stub
	}

	@Override
	public void populateDAG(DAG dag, Configuration conf) {
		// TODO Auto-generated method stub
		HWOperator newOP = dag.addOperator("Hello WORLD", HWOperator.class);
		
		ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
		dag.addStream("consolestream", null, console.input);
		
	}


	
}
