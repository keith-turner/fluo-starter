package ft.persistence;

import java.util.Objects;

import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;

public class ClusterCoefficientIndex {

  public static Column CCI_COL = new Column("cc", "idx");

  public static void updateIndex(TransactionBase tx, String tagName, Double currentCC,
      Double newCC) {

    if (!Objects.equals(currentCC, newCC)) {
      if (currentCC != null) {
        tx.delete(String.format("cci:%.03f:%s", 1.0 - currentCC, tagName),
            ClusterCoefficientIndex.CCI_COL);
      }

      if (newCC != null) {
        tx.set(String.format("cci:%.03f:%s", 1.0 - newCC, tagName), ClusterCoefficientIndex.CCI_COL,
            "");
      }
    }
  }

  public static void printIndex(Snapshot snap) {
    for (RowColumnValue rcv : snap.scanner().over(Span.prefix("cci:")).build()) {
      String[] tuple = rcv.getsRow().split(":");
      double cc = 1 - Double.parseDouble(tuple[1]);
      String tag = tuple[2];

      System.out.printf("%.03f : %s\n", cc, tag);
    }
  }


}
