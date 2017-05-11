package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WordNormalizer extends BaseBasicBolt {

    private List<String> results;
    private FileReader fileReader;

    public void prepare(Map stormConf, TopologyContext context) {
        results = new ArrayList<String>();
//        String sql = null;
//        try {
//            this.fileReader = new FileReader(stormConf.get("table_struct").toString());
//            //Open the reader
//            BufferedReader reader = new BufferedReader(fileReader);
//            sql = reader.readLine();
//            System.out.println(sql);
//
//        } catch (FileNotFoundException e) {
//            throw new RuntimeException("Error reading file [" + stormConf.get("table_struct") + "]");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        if (sql != null){
//            String[] sqlItems=sql.split("where");
//
//        }
    }

    public void cleanup() {
        for (String item : results) {
            System.out.println(item);
        }
    }

    /**
     * The bolt will receive the line from the
     * words file and process it to Normalize this line
     * <p>
     * The normalize will be put the words in lower case
     * and split the line to get all words in this
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        String sentence = input.getString(0);
        String[] words = sentence.split("\\|");
        if (Integer.valueOf(words[1]) > 2) {
            String ansStr = "";
            for (int i = 0; i < words.length - 1; i++) {
                ansStr += words[i] + ",";
            }
            ansStr += words[words.length - 1];
            results.add(ansStr);
        }

    }


    /**
     * The bolt will only emit the field "word"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
//		declarer.declare(new Fields("word"));
//	}
}
