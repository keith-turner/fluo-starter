package ft.logic;

import ft.persistence.ClusterCoefficientIndex;
import ft.persistence.Tag;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;

public class TagObserver extends AbstractObserver {

  private Double clusterCoefficient(Integer links, Integer degree) {
    if (links == null || degree == null || degree == 0) {
      return null;
    }

    return (double) links / (double) (degree * (degree - 1) / 2);
  }

  @Override
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
    Tag tag = new Tag(tx, row, col);

    Integer currentDegree = tag.getDegree();
    Integer currentLinkedUsers = tag.getLinkedUsers();

    int newDegree = currentDegree == null ? 0 : currentDegree + tag.countDegreeUpdates();
    int newLinkedUsers =
        currentLinkedUsers == null ? 0 : currentLinkedUsers + tag.countNewUserEdges();

    tag.deleteUpdates();

    tag.setDegree(newDegree);
    tag.setLinkedUsers(newLinkedUsers);

    ClusterCoefficientIndex.updateIndex(tx, tag.getName(),
        clusterCoefficient(currentLinkedUsers, currentDegree),
        clusterCoefficient(newLinkedUsers, newDegree));
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(Tag.NTFY_COL, NotificationType.WEAK);
  }

}
