/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.twitter;

import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;

import com.datatorrent.contrib.twitter.TwitterSampleInput;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;


/**
 * This application extracts the top urls from twitter that are mentioned in domainlist.txt
 * {@link com.datatorrent.demos.twitter.TwitterTopCounterWithFilterApplication} <br>
 * Run Sample :
 *
 * <pre>
 * 2013-06-17 16:50:34,911 [Twitter Stream consumer-1[Establishing connection]] INFO  twitter4j.TwitterStreamImpl info - Connection established.
 * 2013-06-17 16:50:34,912 [Twitter Stream consumer-1[Establishing connection]] INFO  twitter4j.TwitterStreamImpl info - Receiving status stream.
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * </pre>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name="TwitterDemoWithFilter")
public class TwitterTopCounterWithFilterApplication implements StreamingApplication
{
  private final Locality locality = null;

  private InputPort<Object> consoleOutput(DAG dag, String operatorName)
  {
    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    if (!StringUtils.isEmpty(gatewayAddress)) {
      URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
      String topic = "demos.twitter." + operatorName;
      //LOG.info("WebSocket with gateway at: {}", gatewayAddress);
      PubSubWebSocketOutputOperator<Object> wsOut = dag.addOperator(operatorName, new PubSubWebSocketOutputOperator<Object>());
      wsOut.setUri(uri);
      wsOut.setTopic(topic);
      return wsOut.input;
    }
    ConsoleOutputOperator operator = dag.addOperator(operatorName, new ConsoleOutputOperator());
    operator.setStringFormat(operatorName + ": %s");
    return operator.input;
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Setup the operator to get the data from twitter sample stream injected into the system.
    TwitterSampleInput twitterFeed = new TwitterSampleInput();
    twitterFeed = dag.addOperator("TweetSampler", twitterFeed);

    //  Setup the operator to get the URLs extracted from the twitter statuses with the specified filter
    TwitterURLExtractorWithFilter urlExtractor = dag.addOperator("URLExtractor", TwitterURLExtractorWithFilter.class);

    // Setup a node to count the unique urls within a window.
    UniqueCounter<String> uniqueCounter = dag.addOperator("UniqueURLCounter", new UniqueCounter<String>());

    // Get the aggregated url counts and count them over last 5 mins.
    WindowedTopCounter<String> topCounts = dag.addOperator("TopCounter", new WindowedTopCounter<String>());
    topCounts.setTopCount(20);
    topCounts.setSlidingWindowWidth(1200, 1);

    // Feed the statuses from feed into the input of the url extractor.
    dag.addStream("TweetStream", twitterFeed.status, urlExtractor.input).setLocality(Locality.CONTAINER_LOCAL);
    //  Start counting the urls coming out of URL extractor
    dag.addStream("TwittedURLs", urlExtractor.url, uniqueCounter.data).setLocality(locality);
    // Count unique urls
    dag.addStream("UniqueURLCounts", uniqueCounter.count, topCounts.input).setLocality(locality);
    // Count top 10
    dag.addStream("TopURLs", topCounts.output, consoleOutput(dag, "topURLs")).setLocality(locality);

  }

}
