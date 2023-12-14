package KSPOnStorm;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class Partition implements Serializable {
	public List <Node> boundaryNodeList;
	//HashSet <HypotheticalPath> hypopathSet;
	static int commonId=0;
	public int partitionId=0;
	public HashMap <Node,List<Node>> subGraph;
	public HashMap<Points,Double> subEdgeMap;
	int maxWeight=0;
	int minWeight=0;
	public Partition()
	{
		partitionId=commonId+1;
		commonId++;
		this.boundaryNodeList=new ArrayList <Node>();
		this.subGraph=new HashMap <Node,List<Node>> ();
		this.subEdgeMap=new HashMap <Points,Double>();
	}
}
