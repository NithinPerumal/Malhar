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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;

import java.util.Arrays;
import java.util.HashSet;

import twitter4j.Status;
import twitter4j.User;

/**
 * <p>TwitterStatusUserExtractor class.</p>
 *
 * @since 0.3.2
 */
public class TwitterStatusUserExtractor extends BaseOperator
{
  public HashSet<String> filterList;

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  public final transient DefaultInputPort<User> input = new DefaultInputPort<User>()
  {
    @Override
    public void process(User use)
    {
      String user = use.getScreenName();
      output.emit(user);
    }
  };

}
