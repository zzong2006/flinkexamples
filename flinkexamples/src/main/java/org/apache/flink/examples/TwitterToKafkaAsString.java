/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.examples;

import org.apache.flink.utils.TwitterExampleData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Implements the "TwitterStream" program that computes a most used word
 * occurrence over JSON objects in a streaming fashion.
 *
 * <p>The input is a Tweet stream from a TwitterSource.
 *
 * <p>Usage: <code>Usage: TwitterExample [--output &lt;path&gt;]
 * [--twitter-source.consumerKey &lt;key&gt; --twitter-source.consumerSecret &lt;secret&gt; --twitter-source.token &lt;token&gt; --twitter-source.tokenSecret &lt;tokenSecret&gt;]</code><br>
 *
 * <p>If no parameters are provided, the program is run with default data from
 * {@link TwitterExampleData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>acquire external data,
 * <li>use in-line defined functions,
 * <li>handle flattened stream inputs.
 * </ul>
 */
public class TwitterToKafkaAsString {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************
    private static String LOCAL_KAFKA_BROKER = "10.0.1.25:9092,10.0.2.19:9092,10.0.0.138:9092";
    public static String CLEANSED_RIDES_TOPIC = "twitterText";

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("Usage: TwitterToKafkaAsString [--output <path>] " +
                "[--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> " +
                "--twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret>] " +
                "[--kafka-broker <server> --topic <string>]");

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(params.getInt("parallelism", 1));

        if (params.has("kafka-broker"))
            LOCAL_KAFKA_BROKER = params.get("kafka-broker");
        if (params.has("topic"))
            CLEANSED_RIDES_TOPIC = params.get("topic");

        DataStream<String> streamSource;

        // get input data (From User's parameters)
        if (params.has(TwitterSource.CONSUMER_KEY) &&
                params.has(TwitterSource.CONSUMER_SECRET) &&
                params.has(TwitterSource.TOKEN) &&
                params.has(TwitterSource.TOKEN_SECRET)
        ) {
            streamSource = env.addSource(new TwitterSource(params.getProperties()));
        } else {
            System.out.println("Executing TwitterStream example with default props.");
            // get default test text data
            Properties props = new Properties();
            props.setProperty(TwitterSource.CONSUMER_KEY, "blZVdNb6tLjEHeKDlq9wKvg5S");
            props.setProperty(TwitterSource.CONSUMER_SECRET, "9c3dSVLAloU0CGGP0ROj92AI2pcxhvFPf2uFJa3abdW7cXVxEq");
            props.setProperty(TwitterSource.TOKEN, "3879215892-sVF0AHCoVB3wJKkW7sPn6X8HFJKhNrklX4NctnE");
            props.setProperty(TwitterSource.TOKEN_SECRET, "c1uWi8y4DVCEPVEFVhQv1mQ5I3FU5OHgG87W39UmFn5Mr");
            streamSource = env.addSource(new TwitterSource(props));
        }

        DataStream<String> tweets = streamSource
                // selecting Korean tweets and filtering only text value
                .flatMap(new SelectKoreanAndFilterOnlyText());

        tweets.addSink(new FlinkKafkaProducer<>(
                LOCAL_KAFKA_BROKER,
                CLEANSED_RIDES_TOPIC,
                new SimpleStringSchema()));

        // execute program
        env.execute("Twitter Streaming Example");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Deserialize JSON from twitter source
     *
     * <p>Implements a string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     */
    public static class SelectKoreanAndFilterOnlyText implements FlatMapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        private transient ObjectMapper jsonParser;

        /**
         * Select the language from the incoming JSON text.
         */
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            boolean isKorean = jsonNode.has("user") && jsonNode.get("user").
                                        has("lang") && jsonNode.get("user").
                                        get("lang").asText().equals("ko");
            boolean hasText = jsonNode.has("text");

            if (isKorean && hasText) {
                // message of tweet
                out.collect(jsonNode.get("text").asText());
            }
        }
    }

}