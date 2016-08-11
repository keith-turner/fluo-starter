package ft;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;

public abstract class NewEdgeObserver extends AbstractObserver {

  public NewEdgeObserver(String edgeType) {
    
  }
  
  @Override
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
    
  }

  
  @Override
  public ObservedColumn getObservedColumn() {
    // TODO Auto-generated method stub
    return null;
  }

  public abstract void processNewEdge(TransactionBase tx, Edge edge);
}
