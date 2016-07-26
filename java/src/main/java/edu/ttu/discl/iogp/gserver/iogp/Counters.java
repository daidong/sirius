package edu.ttu.discl.iogp.gserver.iogp;

public class Counters {
	public int alo = 0, ali = 0, plo = 0, pli = 0;
	public int last_reassign_threshold = 0;
	public int edges = 0;
	public Counters(int threshold){
		last_reassign_threshold = threshold;
	}
}
