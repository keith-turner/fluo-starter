package ft;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.fluo.api.client.TransactionBase;

public class Graph {

  public Graph(TransactionBase tx) {
    
  }
  
  public Set<Edge> getExistingEdges(Set<Edge> possibleNewEdges) {
    // TODO Auto-generated method stub
    return null;
  }

  public void addEdges(Set<Edge> edgesToAdd) {
    // TODO Auto-generated method stub
    
  }

  public Set<String> getNeighbors(String edgeType, String node1) {
    // TODO Auto-generated method stub
    return null;
  }

  public void lockNode(String node1) {
    // TODO Auto-generated method stub
    
  }

  public void notifyNodeOfLinkedNeigbors(String tag, Edge edge) {
    // TODO Auto-generated method stub
    
  }

  public Set<Edge> getProcessedEdges(HashSet<Edge> edges) {
    return null;
  }
}
