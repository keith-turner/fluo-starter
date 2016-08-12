package ft.model;

import java.util.Objects;

public class Edge {
  
  private final String node1;
  private final String node2;
  private final EdgeType type;
  
  public Edge(EdgeType type, String node1, String node2) {
    Objects.requireNonNull(type);
    Objects.requireNonNull(node1);
    Objects.requireNonNull(node2);
    
    int cmp = node1.compareTo(node2);
    
    if(cmp == 0) {
      throw new IllegalArgumentException("Self edges not supported");
    }
    
    this.type = type;
    
    if(cmp < 0) {
      this.node1 = node1;
      this.node2 = node2;  
    }else{
      this.node1 = node2;
      this.node2 = node1;
    }
  }
  
  public String getNode1() {
    return node1;
  }
  
  public String getNode2() {
    return node2;
  }

  public EdgeType getType() {
    return type;
  }
  
  @Override
  public int hashCode(){
    return Objects.hash(node1, node2, type);
  }
  
  @Override
  public boolean equals(Object o) {
    if(o instanceof Edge) {
      Edge oe = (Edge)o;
      return node1.equals(oe.node1) && node2.equals(oe.node2) && type.equals(oe.type);
    }
    
    return false;
  }
}
