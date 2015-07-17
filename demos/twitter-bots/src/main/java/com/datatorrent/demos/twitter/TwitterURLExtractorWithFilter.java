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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import twitter4j.Status;
import twitter4j.URLEntity;

/**
 * <p>TwitterStatusURLExtractorWithFilter class.</p>
 *
 * @since 0.3.2
 */
public class TwitterURLExtractorWithFilter extends BaseOperator
{

  protected String fileName = "/home/dtadmin/repos/twitter-bots/src/main/java/com/datatorrent/demos/twitter/domainlist.txt";
  protected List<String> myList = new ArrayList<String>();

  public final transient DefaultOutputPort<String> url = new DefaultOutputPort<String>();
  public final transient DefaultInputPort<Status> input = new DefaultInputPort<Status>()
  {
    @Override
    public void process(Status status)
    {
      if (myList.isEmpty()) { //If condition so buffered reader is not used repeatedly.
      BufferedReader in = null;
      try {   
          in = new BufferedReader(new FileReader(fileName)); //Errors with the buffered reader.
          String str;
          while ((str = in.readLine()) != null) {
              myList.add(str);
          }
	      } catch (FileNotFoundException e) {
	          e.printStackTrace();
	      } catch (IOException e) {
	          e.printStackTrace();
	      } finally {
	          if (in != null) {
	              try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
	          }
	      }
      }
      URLEntity[] entities = status.getURLEntities();
      if (entities != null) {
        for (URLEntity ue: entities) {
          if (ue != null) { // see why we intermittently get NPEs
        	  String toEmit = (ue.getExpandedURL() == null ? ue.getURL() : ue.getExpandedURL()).toString();
        	  for (String word : myList) {
//        		  System.out.println("CURRENT WORD:                " + word);
        		  if (toEmit.contains(word)) {
//        			  System.out.println("EMITTED			" + toEmit);
        			  url.emit(toEmit);
        		  }
        	  }
          }
        }
      }
    }
  };
}
