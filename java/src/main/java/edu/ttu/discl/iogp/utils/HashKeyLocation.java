package edu.ttu.discl.iogp.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class HashKeyLocation {

	public static int getEdgeLocation(byte[] src, int serverNum){
		int m = serverNum;
		JenkinsHash jh = new JenkinsHash();
		int hashi = Math.abs(jh.hash32(src));
		//System.out.println("key: " + new String(src) + " srvs: " + m + " rtn: " + (hashi % m));
		return (hashi % m);
	}
	
	public static Set<Integer> getGridEdgeLocs(byte[] src, int serverNum){
		HashSet<Integer> reqs = new HashSet<Integer>();
		JenkinsHash jh = new JenkinsHash();
		int m = serverNum;
		int sqrt = (int) Math.sqrt(m);
		int hashi = Math.abs(jh.hash32(src));
		ArrayList<Integer> ilocs = new ArrayList<Integer>();
		
		int irow = (hashi % m) / sqrt;
		int icol = (hashi % m) % sqrt;
		
		for (int w = 0; w < sqrt; w++){
			ilocs.add((irow * sqrt + w) % m);
			ilocs.add((w * sqrt + icol) % m);
		}
		
		for (int loc : ilocs){
			reqs.add(loc);
		}
		return reqs;
	}
}
