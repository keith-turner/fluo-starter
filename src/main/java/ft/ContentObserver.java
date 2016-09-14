package ft;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.observer.AbstractObserver;

public class ContentObserver extends AbstractObserver {

  private Set<String> tokenize(String content) {
    return new HashSet<String>(Arrays.asList(content.split("[ .!,]+")));
  }

  public static final Column PROCESSED_COL = new Column("doc", "processed");
  public static final Column TOTAL_WORD_COUNT = new Column("word","docCount"); 
  
  @Override
  public void process(TransactionBase tx, Bytes brow, Column col) throws Exception {

    String row = brow.toString();
    
    Map<Column, String> colVals =
        tx.gets(row, ImmutableSet.of(DocLoader.CONTENT_COL, DocLoader.REF_STATUS_COL, PROCESSED_COL));
    String content = colVals.get(DocLoader.CONTENT_COL);
    String status = colVals.get(DocLoader.REF_STATUS_COL);
    String processed = colVals.getOrDefault(PROCESSED_COL, "false");

    if (status.equals("referenced")) {
      if(processed.equals("false")) {
        tx.set(row, PROCESSED_COL, "true");
        adjustCounts(tx, 1, tokenize(content));
      }
    } else if (status.equals("unreferenced")) {
      if(processed.equals("true")) {
        adjustCounts(tx, -1, tokenize(content));
      }
    
      tx.delete(brow, PROCESSED_COL);
      tx.delete(brow, DocLoader.CONTENT_COL);
      tx.delete(brow, DocLoader.REF_COUNT_COL);
      tx.delete(brow, DocLoader.REF_STATUS_COL);
    }
  }

  private void adjustCounts(TransactionBase tx, int delta, Set<String> words) {
    Collection<RowColumn> wordRowCols = Collections2.transform(words, w -> new RowColumn("w:"+w, TOTAL_WORD_COUNT));
    Map<RowColumn, String> currentCounts = tx.gets(wordRowCols);
   
    for (RowColumn rc : wordRowCols) {
      int currentCount = Integer.parseInt(currentCounts.getOrDefault(rc, "0"));
      int newCount = currentCount + delta;
      if(newCount == 0) {
        tx.delete(rc.getRow(), rc.getColumn());
      } else {
        tx.set(rc.getsRow(), rc.getColumn(), "" + newCount);
      }
    }
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(DocLoader.REF_STATUS_COL, NotificationType.STRONG);
  }
}
