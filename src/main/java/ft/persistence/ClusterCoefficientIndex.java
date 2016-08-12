package ft.persistence;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Column;

public class ClusterCoefficientIndex {

  public static void updateIndex(TransactionBase tx, String tagName, int currentDegree, int currentLinkedUsers,
      int newDegree, int newLinkedUsers) {
    if(currentDegree > 0) {
      double currentCC = (double) currentLinkedUsers / (double) currentDegree;
      tx.delete(String.format("cci:%0.3f:%s", currentCC, tagName), ClusterCoefficientIndex.CCI_COL);
    }
    
    if(newDegree > 0) {
      double newCC = (double) newLinkedUsers / (double) newDegree;
      tx.set(String.format("cci:%0.3f:%s", newCC, tagName), ClusterCoefficientIndex.CCI_COL, "");
    }
  }

  public static Column CCI_COL = new Column("cc","idx");

}
