package KSPOnStorm;

import java.io.Serializable;

public class Points implements Serializable {

	public int snd;
	public int tnd;
	public Points()
	{

	}
	public Points(int snd, int tnd)
	{
		this.snd=snd;
		this.tnd=tnd;
	}
	 @Override
	 public int hashCode()
	  {
		  return snd*37+tnd;
	  }
	  @Override
	    public boolean equals(Object obj) {
	        if (obj instanceof Points) {
	            Points ps= (Points) obj;
	            if (ps.snd==this.snd && ps.tnd==this.tnd)
	            {
	            	return true;
	            }
	        }
	        return false;
	    }
}
