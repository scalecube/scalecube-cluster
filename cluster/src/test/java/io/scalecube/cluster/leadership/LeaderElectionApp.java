package io.scalecube.cluster.leadership;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.leaderelection.RaftLeaderElection;

public class LeaderElectionApp {

	public static void main(String[] args) {
		
		Cluster cluster  = Cluster.joinAwait();	
		cluster.leadership("test1").start().subscribe();
		
		Cluster cluster1  = Cluster.joinAwait(cluster.address());	
		cluster1.leadership("test1").start().subscribe();
		
		Cluster cluster2  = Cluster.joinAwait(cluster.address());	
		cluster2.leadership("test1").start().subscribe();
		
		System.out.println("done");
		
	}

}
