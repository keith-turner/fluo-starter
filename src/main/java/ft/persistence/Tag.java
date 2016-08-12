package ft.persistence;

import java.util.ArrayList;

import com.google.common.collect.Iterables;
import ft.model.Edge;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;

public class Tag {
  
  private static final String PREFIX = "t:";
  
  private static final String NEW_USER_LINK_FAM = "nul";
  private static final String DEGREE_UPDATE_FAM = "du";

  private static final Column DEGREE_COL = new Column("stat","degree");
  private static final Column LINKED_USERS_COL  = new Column("stat","linkedUsers");
  
  public static final Column NTFY_COL = new Column("tag","update");
  
  private TransactionBase tx;
  private String tag;
  private String row;

  //following variable store the state of tag as read from a transaction
  private ArrayList<RowColumnValue> linkedUserUpdates;
  private ArrayList<RowColumnValue> degreeUpdates;
  Integer degree;
  Integer linkedUsers;
  
  private void readState(){
    //all of tags data is stored in one row, whenever trying to read any of it... read all of it.
    
    if(linkedUserUpdates == null) {
      linkedUserUpdates = new ArrayList<>();
      degreeUpdates = new ArrayList<>();
      degree = 0;
      linkedUsers = 0;
      
      for(RowColumnValue rcv : tx.scanner().over(Span.exact(row)).build()){
        String family = rcv.getColumn().getsFamily();
        
        if(family.equals(NEW_USER_LINK_FAM)) {
          linkedUserUpdates.add(rcv);
        } else if(family.equals(DEGREE_UPDATE_FAM)) {
          degreeUpdates.add(rcv);
        } else if(rcv.getColumn().equals(DEGREE_COL)) {
          degree = Integer.parseInt(rcv.getsValue());
        } else if(rcv.getColumn().equals(LINKED_USERS_COL)) {
          linkedUsers = Integer.parseInt(rcv.getsValue());
        } else {
          throw new IllegalArgumentException("Bad column :"+rcv.getColumn());
        }
      }
    }
    
  }
  
  public Tag(TransactionBase tx, String tag){
    this.tx = tx;
    this.tag = tag;
    this.row = PREFIX+tag;
  }
  
  
  public Tag(TransactionBase tx, Bytes row, Column col) {
    this.row = row.toString();
    this.tx = tx;
    this.tag = this.row.substring(PREFIX.length());
  }


  public void queueNewUserEdge(Edge newUserEdge){
    String encodedEdge = Graph.toRow(newUserEdge);
    tx.set(row, new Column(NEW_USER_LINK_FAM, encodedEdge), "");
  }
  
  public void queueDegreeUpdate(String user){
    tx.set(row, new Column(DEGREE_UPDATE_FAM, user), "");
  }
  
  public void weaklyNotify(){
    tx.setWeakNotification(row, NTFY_COL);
  }
  
  
  public int countNewUserEdges(){
    readState();
    return linkedUserUpdates.size();
  }
  
  public int countDegreeUpdates() {
    readState();
    return degreeUpdates.size();
  }

  public Integer getDegree(){
    readState();
    return degree;
  }
  
  public void setDegree(int degree) {
    tx.set(row, DEGREE_COL, degree+"");
  }
  
  public Integer getLinkedUsers(){
    readState();
    return linkedUsers;
  }
  
  public void setLinkedUsers(int linkedUsers) {
    tx.set(row, LINKED_USERS_COL, linkedUsers+"");
  }


  public String getName() {
    return tag;
  }


  public void deleteUpdates() {
    for(RowColumnValue rcv : Iterables.concat(degreeUpdates, linkedUserUpdates)){
      tx.delete(rcv.getRow(), rcv.getColumn());
    }
  }
}
