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


import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.twitter.TwitterSampleInput;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import org.apache.hadoop.conf.Configuration;

/**
 * This application extracts the top users from twitter
 * {@link com.datatorrent.demos.twitter.TwitterTopUsersApplication} <br>
 * Run Sample :
 *
 * <pre>
 * 2013-06-17 16:50:34,911 [Twitter Stream consumer-1[Establishing connection]] INFO  twitter4j.TwitterStreamImpl info - Connection established.
 * 2013-06-17 16:50:34,912 [Twitter Stream consumer-1[Establishing connection]] INFO  twitter4j.TwitterStreamImpl info - Receiving status stream.
 * {kiryasjoelinfo=1, ash_dans=1, NolanSchlange=1, MadisonPunzo=1, neslisahsen_=1, _DaRealJayee=2, Nash_Malibu=1, peacecompassion=1}
 * </pre>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name="RollingTopUsersDemo")
public class TwitterTopUsersApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
      TwitterSampleInput twitterFeed = new TwitterSampleInput();
      twitterFeed = dag.addOperator("TweetSampler", twitterFeed);

      TwitterStatusUserExtractor userExtractor = dag.addOperator("UserExtractor", TwitterStatusUserExtractor.class);
      UniqueCounter<String> uniqueCounter = dag.addOperator("UniqueWordCounter", new UniqueCounter<String>());
      WindowedTopCounter<String> topCounts = dag.addOperator("TopCounter", new WindowedTopCounter<String>());
      topCounts.setSlidingWindowWidth(600, 1);

      dag.addStream("TweetStream", twitterFeed.userMention, userExtractor.input);
      dag.addStream("TwittedWords", userExtractor.output, uniqueCounter.data);
      dag.addStream("UniqueWordCounts", uniqueCounter.count, topCounts.input);

      ConsoleOutputOperator consoleOperator = dag.addOperator("topWords", new ConsoleOutputOperator());
      dag.addStream("TopWords", topCounts.output, consoleOperator.input);
  }
}
