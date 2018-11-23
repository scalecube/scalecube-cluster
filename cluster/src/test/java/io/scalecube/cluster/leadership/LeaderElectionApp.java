package io.scalecube.cluster.leadership;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.LeaderElection;

public class LeaderElectionApp {

	public static void main(String[] args) {
		
		Cluster cluster  = Cluster.joinAwait();	
		LeaderElection le1 = cluster.leadership("test1");;	
		le1.listen().subscribe(e->{
			System.out.println("Alice -> " + e.state());
		});
		
		
		Cluster cluster1  = Cluster.joinAwait(cluster.address());	
		LeaderElection le2 = cluster1.leadership("test1");;
		le2.listen().subscribe(e->{
			System.out.println("Johan -> " + e.state());
		});
		
		
		Cluster cluster2  = Cluster.joinAwait(cluster.address());	
		LeaderElection le3= cluster2.leadership("test1");;
		le3.listen().subscribe(e->{
			System.out.println("David -> " + e.state());
		});
		
		le1.start().subscribe();
		le2.start().subscribe();
		le3.start().subscribe();
		
		System.out.println("done");
		
	}

}
