package KSPOnStorm;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Node implements Comparable<Node>, Cloneable,Serializable {

	public int id;
	char visitFlag;
	Map<String, Boolean> dijMap;
	//Map<String, Boolean> yenFlagMap;
//	boolean dijFlag; //for dijkstra
	Double dijkstraDistance;// = Double.MAX_VALUE;
	public Node()
	{
//		this.dijFlag=false;
        dijMap = new ConcurrentHashMap<>();
        //yenFlagMap=new ConcurrentHashMap<>();
	}
	public Node(int Nodeid)
	{
		this.id=Nodeid;
//		this.dijFlag=false;
        dijMap = new ConcurrentHashMap<>();
        //yenFlagMap=new ConcurrentHashMap<>();
	}

	@Override
	public Object clone()
	{
		Node node=null;
		try{
			node=(Node)super.clone();
		}catch(CloneNotSupportedException e)
		{
			e.printStackTrace();
		}
		return node;
	}
	@Override
	public int hashCode()
	{
		Node node=(Node)this;
		return node.id;
	}
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Node) {
			Node node= (Node) obj;
			if (node.id==this.id)
			{
				return true;
			}
		}
		return false;
	}
	@Override
	public int compareTo(Node node) {
		// TODO Auto-generated method stub

		int cmp = Double.compare(this.dijkstraDistance, ((Node) node).dijkstraDistance);
		return cmp;
	}

	public boolean getDij(String id) {
	    if(!dijMap.containsKey(id)) {
                dijMap.put(id, false);
        }
        return dijMap.get(id);
    }

    public void setDij(String id, boolean flag) {
	    dijMap.put(id, flag);
    }

//	public boolean getYenFlag(String id) {
//		if(!yenFlagMap.containsKey(id)) {
//			yenFlagMap.put(id, false);
//		}
//		return yenFlagMap.get(id);
//	}
//
//	public void setYenFlag(String id, boolean flag) {
//		yenFlagMap.put(id, flag);
//	}

	@Override
	public String toString() {
		return "Node{" +
				"id=" + id +
				", visitFlag=" + visitFlag +
				", dijMap=" + dijMap +
				", dijkstraDistance=" + dijkstraDistance +
				'}';
	}
}
