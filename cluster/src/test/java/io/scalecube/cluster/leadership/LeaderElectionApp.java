package io.scalecube.cluster.leadership;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.LeaderElection;

public class LeaderElectionApp {

	public static void main(String[] args) {

		Cluster cluster = Cluster.joinAwait();
		Cluster cluster1 = Cluster.joinAwait(cluster.address());
		Cluster cluster2 = Cluster.joinAwait(cluster.address());
		
		LeaderElection le1 = cluster.leadership("test1");
		LeaderElection le2 = cluster1.leadership("test1");
		LeaderElection le3 = cluster2.leadership("test1");
		
		le1.listen().subscribe(e -> {
			System.out.println("Alice " + le1.state() + " -> " + e.state());
			print(le1,le2,le3);
		});

		le2.listen().subscribe(e -> {
			System.out.println("Joe " + le1.state() + " -> " + e.state());
			print(le1,le2,le3);
		});

		le3.listen().subscribe(e -> {
			System.out.println("David " + le1.state() + " -> " + e.state());
			print(le1,le2,le3);
		});

		le1.start().subscribe();
		le2.start().subscribe();
		le3.start().subscribe();

		System.out.println("done");

	}

	public static void print(LeaderElection... elections ) {
		for(LeaderElection election : elections) {
			System.out.println( election.state());
		}
	}
}
