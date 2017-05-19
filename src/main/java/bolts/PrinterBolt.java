/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;


public class PrinterBolt extends BaseBasicBolt {

  private List<Integer> results = new ArrayList<Integer>();
  private int ans = 0;
  @Override
  public void cleanup() {
    super.cleanup();


  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {

//    System.out.println("lilele: "+tuple);
    System.out.println("print bolt **********************************");
    for(String string:tuple.getFields()){
        System.out.println(string);
        System.out.println(tuple.getValueByField(string));
    }



  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }

}
