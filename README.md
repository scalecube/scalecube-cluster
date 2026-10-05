# ScaleCube - Cluster

ScaleCube Cluster is a lightweight decentralized cluster membership, failure detection, and gossip
protocol library.
It provides an implementation
of [SWIM](http://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)  cluster membership
protocol for Java VM.

It is an efficient and scalable weakly-consistent distributed group membership protocol based on
gossip-style communication between the
nodes in the cluster.
Read [blog post](http://www.antonkharenko.com/2015/09/swim-distributed-group-membership.html) with
distilled
notes on the SWIM paper for more details.

It is using
a [random-probing failure detection algorithm](http://www.antonkharenko.com/2015/08/scalable-and-efficient-distributed.html)
which provides
a uniform expected network load at all members.
The worst-case network load is linear O (n) for overall network produced by running algorithm on all
nodes and constant network
load at one particular member independent from the size of the cluster.

ScaleCube Cluster implements all improvements described at original SWIM algorithm paper, such as
gossip-style dissemination, suspicion mechanism
and time-bounded strong completeness of failure detector algorithm. In addition to that we have
introduced support of additional SYNC mechanism
in order to improve recovery of the cluster from network partitioning events.

Using ScaleCube Cluster as simple as few lines of code:

``` java
// Start cluster node Alice as a seed node of the cluster, listen and print all incoming messages
Cluster alice = Cluster.joinAwait();
alice.listen().subscribe(msg -> System.out.println("Alice received: " + msg.data()));

// Join cluster node Bob to cluster with Alice, listen and print all incoming messages
Cluster bob = Cluster.joinAwait(alice.address());
bob.listen().subscribe(msg -> System.out.println("Bob received: " + msg.data()));

// Join cluster node Carol to cluster with Alice (and Bob which is resolved via Alice)
Cluster carol = Cluster.joinAwait(alice.address());

// Send from Carol greeting message to all other cluster members (which is Alice and Bob)
carol.otherMembers().forEach(member -> carol.send(member, Message.fromData("Greetings from Carol")));
```

You are welcome to explore javadoc documentation on cluster API and examples module for more
advanced use cases.

## Configuration

`ClusterConfig` and its nested configs (`TransportConfig`, `FailureDetectorConfig`, `GossipConfig`,
`MembershipConfig`) are plain mutable objects: setters change the instance and return it. Every
scalar setting is read from `java.util.Properties`:

``` java
new ClusterImpl(properties)          // or new ClusterConfig(properties)
new ClusterImpl()                    // reads System.getProperties(), i.e. -D flags
```

A property set to `@null` counts as not set, so a layered config can unset an inherited value.
Explicit setters called after construction win over properties.

| Key                                                                                                                           | Default                                                                    |
|-------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------|
| `scalecube.cluster.memberId`                                                                                                  | generated UUID                                                             |
| `scalecube.cluster.memberAlias`                                                                                               | none                                                                       |
| `scalecube.cluster.externalHost` / `externalPort`                                                                             | transport's bound address                                                  |
| `scalecube.cluster.metadataTimeout`                                                                                           | `ClusterConfig.DEFAULT_METADATA_TIMEOUT`                                   |
| `scalecube.cluster.transport.port` / `clientSecured` / `connectTimeout` / `maxFrameLength`                                    | `TransportConfig.DEFAULT_*`                                                |
| `scalecube.cluster.failureDetector.pingInterval` / `pingTimeout` / `pingReqMembers`                                           | `FailureDetectorConfig.DEFAULT_*`                                          |
| `scalecube.cluster.gossip.gossipFanout` / `gossipInterval` / `gossipRepeatMult` / `gossipSegmentationThreshold`               | `GossipConfig.DEFAULT_*`                                                   |
| `scalecube.cluster.membership.seedMembers` (comma-separated) / `syncInterval` / `syncTimeout` / `suspicionMult` / `namespace` | `MembershipConfig.DEFAULT_*`                                               |
| `scalecube.cluster.transport.transportFactory` (class name)                                                                   | first `ServiceLoader` provider (websocket from transport-netty), else none |
| `scalecube.cluster.transport.messageCodec` / `scalecube.cluster.metadataCodec` (class name)                                   | first `ServiceLoader` provider, else JDK                                   |

Times are milliseconds. Class-name settings are instantiated through a public no-arg constructor,
as Aeron does for its suppliers. `metadata` and `addressMapper` are set in code.

## Support

For improvement requests, bugs and discussions please use
the [GitHub Issues](https://github.com/scalecube/scalecube/issues)
or chat with us to get support on [Gitter](https://gitter.im/scalecube/Lobby).

You are more then welcome to join us or just show your support by granting us a small star :)

## Maven

Binaries and dependency information for Maven can be found at
[http://search.maven.org](http://search.maven.org/#search%7Cga%7C1%7Cio.scalecube.scalecube).

To add a dependency on ScaleCube Cluster using Maven, use the following:

``` xml
<dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-cluster</artifactId>
  <version>x.y.z</version>
</dependency>
```

To add a dependency on ScaleCube Transport using Maven, use the following:

``` xml
<dependency>
  <groupId>io.scalecube</groupId>
  <artifactId>scalecube-transport</artifactId>
  <version>x.y.z</version>
</dependency>
```

## Contributing

* Follow/Star us on github.
* Fork (and then git clone https://github.com/--your-username-here--/scalecube.git).
* Create a branch (git checkout -b branch_name).
* Commit your changes (git commit -am "Description of contribution").
* Push the branch (git push origin branch_name).
* Open a Pull Request.
* Thank you for your contribution! Wait for a response...

## References

* Anton Kharenko

  [blog](http://www.antonkharenko.com/)

* Ronen Nachmias

  [posts](https://www.linkedin.com/today/author/ronenhm?trk=pprof-feed)

## License

[Apache License, Version 2.0](https://github.com/scalecube/scalecube/blob/master/LICENSE.txt)
