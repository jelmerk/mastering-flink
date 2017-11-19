package com.demo.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Splitter implements FlatMapFunction<String, Tuple2<String, Double>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(String value, Collector<Tuple2<String, Double>> out) throws Exception {

		// input format : timestamp_millis,value,sensor_name
		System.out.println("In the splitter : " + value);

		if (null != value && value.contains(",")) {
			String parts[] = value.split(",");
			// output format : sensor_name, value
			out.collect(new Tuple2<>(parts[2], Double.parseDouble(parts[1])));
		}
	}

}
