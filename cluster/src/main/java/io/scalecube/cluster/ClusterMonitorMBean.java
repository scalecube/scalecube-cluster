package io.scalecube.cluster;

import java.util.List;
import java.util.Map;

public interface ClusterMonitorMBean {
     String getMember();
     int getIncarnation();
     Map<String, String> getMetadata();
     List<String> getAliveMembers(); // all alive members except local one
     List<String> getDeadMembers(); // let's keep recent N dead members for full-filling this method
     List<String> getSuspectedMembers();
}
