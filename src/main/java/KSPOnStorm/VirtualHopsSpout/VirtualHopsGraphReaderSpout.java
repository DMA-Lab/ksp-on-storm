package KSPOnStorm.VirtualHopsSpout;

import KSPOnStorm.VirtualHopsBolt.VirtualHopsConstructGraphBolt;
import KSPOnStorm.VirtualHopsData.VirtualHopStreamFields;
import KSPOnStorm.VirtualHopsData.VirtualHopStreamIDs;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import KSPOnStorm.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class VirtualHopsGraphReaderSpout extends BaseRichSpout{

    private static final Logger LOG = LoggerFactory.getLogger(VirtualHopsGraphReaderSpout.class);
    SpoutOutputCollector collector;
    public static String finishedFlag="RGF";
    Map<Integer,Node> existNodeMap;
    long startReadTime=0;
    String fileAddress;
    int edgeCount=0;

    public VirtualHopsGraphReaderSpout(String fileAddress) {
        this.fileAddress = fileAddress;
        existNodeMap=new HashMap<Integer,Node>();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        File filename = new File(this.fileAddress); //
        InputStreamReader reader = null;
        try {
            reader = new InputStreamReader(
                    new FileInputStream(filename));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(-1);
        } //
        BufferedReader br = new BufferedReader(reader);
        String line;
        startReadTime=System.currentTimeMillis();
        //int i=0;
        try {
            while ((line = br.readLine())!= null)
            {
                 if(line.charAt(0)=='a')
                {
                    String [] segments=line.split(" ");
                    int firstNodeID=Integer.parseInt(segments[1]);
                    int secondNodeID=Integer.parseInt(segments[2]);
                    Double edgeWeight=Double.parseDouble(segments[3]);

                    Node firstNode=null;
                    if(existNodeMap.keySet().contains(firstNodeID))
                    {
                        firstNode=existNodeMap.get(firstNodeID);
                    }
                    else
                    {
                        firstNode=new Node(firstNodeID);
                        existNodeMap.put(firstNodeID,firstNode);
                    }
                    Node secondNode=null;
                    if(existNodeMap.keySet().contains(secondNodeID))
                    {
                        secondNode= existNodeMap.get(secondNodeID);
                    }
                    else
                    {
                        secondNode=new Node(secondNodeID);
                        existNodeMap.put(secondNodeID,secondNode);
                    }

                    //System.out.println(graph.contains(snd));
                    // emit
                    edgeCount++;
                    this.collector.emit(VirtualHopStreamIDs.SPOUT_ReadGaph, new Values(firstNode , secondNode, edgeWeight)); //send a line of graph to ConstructGraphBolt
                    Runtime.getRuntime().totalMemory();
                    try {                                    //the new code
                        if(edgeCount%100000==0)
                        {
                            Thread.sleep(1);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            this.collector.emit(VirtualHopStreamIDs.fininshReadGraphFlag,new Values(finishedFlag));    //send a flag indicating all lines have been sent
            LOG.info("New version. ReaderSpout finishes reading graph, the time is {}", System.currentTimeMillis()-startReadTime);
            existNodeMap.clear();//clear the existing node map
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {

    }
    @Override
    public void declareOutputFields (OutputFieldsDeclarer declarer){
        declarer.declareStream(VirtualHopStreamIDs.SPOUT_ReadGaph,
                new Fields(VirtualHopStreamFields.Start_Node, VirtualHopStreamFields.End_Node, VirtualHopStreamFields.Edge_Weight));

        declarer.declareStream(VirtualHopStreamIDs.fininshReadGraphFlag,
                new Fields(VirtualHopStreamFields.ReadGraph_Flag));
    }
}
