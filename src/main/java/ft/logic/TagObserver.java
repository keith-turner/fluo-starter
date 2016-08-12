package ft.logic;

import ft.persistence.ClusterCoefficientIndex;
import ft.persistence.Tag;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;

public class TagObserver extends AbstractObserver {

  
  @Override
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
    Tag tag = new Tag(tx, row, col);
    
    int currentDegree = tag.getDegree();
    int currentLinkedUsers = tag.getLinkedUsers();
    
    int newDegree = currentDegree + tag.countDegreeUpdates();
    int newLinkedUsers = currentLinkedUsers + tag.countNewUserEdges();
    
    tag.deleteUpdates();
    
    tag.setDegree(newDegree);
    tag.setLinkedUsers(newLinkedUsers);
    
    
    ClusterCoefficientIndex.updateIndex(tx, tag.getName(), currentDegree, currentLinkedUsers, newDegree, newLinkedUsers);
  }

  @Override
  public ObservedColumn getObservedColumn() {
    // TODO Auto-generated method stub
    return null;
  }

}
