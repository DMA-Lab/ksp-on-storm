package KSPOnStorm;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class Path implements Comparable,Serializable {
	
	Node startNode;
	Node endNode;
	public List<Node> line;
	public double length;
	public int hypoID=0;
	static int commonId=0;
	public boolean hypoFlag=false;
	public Path()
	{

	}
	public Path(List<Node> line, double length)
	{
		this.line=line;
		this.length=length;
		hypoFlag=false;
		hypoID=commonId+1;
		commonId++;
	}
	public Path(Node startNode, Node endNode)
	{
		this.startNode=startNode;
		this.endNode=endNode;
		this.line=new LinkedList<Node>();
	}
	@Override
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		Path nextPath=(Path)arg0;
		return Double.compare(this.length, nextPath.length);
	}
	@Override
	public int hashCode()
	{
		Path path=(Path)this;
		int hops=0;
		for(int i=0;i<path.line.size();i++)
		{
			hops=hops+path.line.get(i).id;
		}
		return hops;
	}
	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof Path) {
			Path path= (Path) obj;
			if (path.line.size()!=this.line.size())
			{
				return false;
			}
			else
			{
				for(int i=0;i<this.line.size();i++)
				{
					if(this.line.get(i).id!=(path.line.get(i).id))
					{
						return false;
					}
					else
					{
						continue;
					}
				}
				return true;
			}
		}
		return false;
	}
}
