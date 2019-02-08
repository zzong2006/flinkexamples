/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.examples;

import org.apache.flink.utils.TaxiRide;
import org.apache.flink.utils.TaxiRideSource;
import org.apache.flink.utils.ExerciseBase;
import org.apache.flink.utils.GeoUtils;
import org.apache.flink.utils.TaxiRideSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;


/**
 * Java reference implementation for the "Ride Cleansing" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City.
 * The resulting stream is written to an Apache Kafka topic.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class RideCleansingToKafka extends ExerciseBase {

	private static final String LOCAL_KAFKA_BROKER = "10.0.1.25:9092,10.0.2.19:9092,10.0.0.138:9092";
	public static final String CLEANSED_RIDES_TOPIC = "cleansedRides";

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		// final String input = params.get("input", pathToRideData);
		String input = params.getRequired("input");	//

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 300; // events of 10 minute are served in 2 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

		DataStream<TaxiRide> filteredRides = rides
				// filter out rides that do not start or stop in NYC
				.filter(new NYCFilter());

		// write the filtered data to a Kafka sink
		filteredRides.addSink(new FlinkKafkaProducer<TaxiRide>(
				LOCAL_KAFKA_BROKER,
				CLEANSED_RIDES_TOPIC,
				new TaxiRideSchema()));

		// run the cleansing pipeline
		env.execute("Taxi Ride Cleansing");
	}


	public static class NYCFilter implements FilterFunction<TaxiRide> {

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}

}
