package ft.persistence;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import ft.model.Edge;
import ft.model.EdgeState;
import ft.model.EdgeType;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;

public class Graph {

  public static final Column EDGE_STATE_COL = new Column("edge","state");
  public static final Column EDGE_ADDTIME_COL = new Column("edge","added");
  
  private TransactionBase tx;
  
  private static String getPrefix(EdgeType type) {
    switch(type){
      case HASHTAG:
        return "eh";
      case USER:
        return "eu";
      default:
        throw new IllegalStateException();
    }
  }
  
  private static EdgeType getType(String prefix) {
    switch(prefix) {
      case "eh":
        return EdgeType.HASHTAG;
      case "eu":
        return EdgeType.USER;
      default:
        throw new IllegalArgumentException("Bad prefix :"+prefix);
    }
  }
  
  private static Edge toEdge(String row) {
    String[] tuple = row.split(":");
    if(tuple.length != 3) {
      throw new IllegalArgumentException("Bad edge row :"+row);
    }
    return new Edge(getType(tuple[0]),tuple[1],tuple[2]);
    
  }
  
  private static void checkNodeId(String nid) {
    Preconditions.checkArgument(!nid.contains(":"), "Node id '%s' cotains ':'", nid);
  }
  
  static String toRow(Edge e){
    checkNodeId(e.getNode1());
    checkNodeId(e.getNode2());
    
    return getPrefix(e.getType()) + ":" + e.getNode1() + ":" + e.getNode2();
  }
  
  private static String toReverseRow(Edge e){
    checkNodeId(e.getNode1());
    checkNodeId(e.getNode2());
    
    return getPrefix(e.getType()) + ":" + e.getNode2() + ":" + e.getNode1();
  }
  
  public Graph(TransactionBase tx) {
    this.tx = tx;
  }

  
  public Map<Edge, EdgeState> getExistingEdges(Set<Edge> edges){
    
    Collection<RowColumn> encodedEdges = Collections2.transform(edges, e -> new RowColumn(toRow(e), EDGE_STATE_COL));
    
    Map<Edge, EdgeState> ret = new HashMap<>();
    for(Entry<RowColumn, String> entry : tx.gets(encodedEdges).entrySet()){
      ret.put(toEdge(entry.getKey().getsRow()), EdgeState.valueOf(entry.getValue()));
    }
    
    return ret;
  }
  
  public void addNewEdges(Set<Edge> edges) {
    
    String addTime = System.currentTimeMillis()+"";
    
    for (Edge edge : edges) {
      if(edge.getType() == EdgeType.HASHTAG) {
        String reverseRow = toReverseRow(edge);
        tx.set(reverseRow, EDGE_STATE_COL, EdgeState.NEW.name());
      }
      
      String row = toRow(edge);
      
      tx.set(row, EDGE_STATE_COL, EdgeState.NEW.name());
      tx.set(row, EDGE_ADDTIME_COL, addTime);
    }
  }
 

  public Set<String> getNeighbors(EdgeType type, String node, EdgeState state) {
    
    if(type != EdgeType.HASHTAG) {
      //only dobule index hashtag edges
      throw new IllegalArgumentException("Neighbor lookup not supported on "+type);
    }
    
    String scanPrefix = getPrefix(type)+":"+node+":";
    
    HashSet<String> neighbors = new HashSet<>();
    
    CellScanner scanner = tx.scanner().over(Span.prefix(scanPrefix)).fetch(EDGE_STATE_COL).build();
    for (RowColumnValue rcv : scanner) {
      if(rcv.getsValue().equals(state.name())) {
        neighbors.add(rcv.getsRow().substring(scanPrefix.length()));
      }
    }

    return neighbors;
  }

  public void setEdgeState(Edge edge, EdgeState state) {
   if(edge.getType() == EdgeType.HASHTAG) {
     String reverseRow = toReverseRow(edge);
     tx.set(reverseRow, EDGE_STATE_COL, state.name());
   }
   
   String row = toRow(edge);
   tx.set(row, EDGE_STATE_COL, state.name());
  }


  public static Edge decodeEdge(Bytes row, Column col) {
    return toEdge(row.toString());
  }


  public void setNodeAttribute(String nodeId, String attr, String value) {
    tx.set("n:"+nodeId, new Column("nattr", attr), value);
  }
}
