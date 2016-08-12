package ft.logic;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import ft.model.Edge;
import ft.model.EdgeState;
import ft.model.EdgeType;
import ft.persistence.Graph;
import ft.persistence.Tag;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;

public class NewEdgeObserver extends AbstractObserver {

  public void processNewHashtagEdge(TransactionBase tx, Edge edge) {

    String tagName = edge.getNode1();
    String user = edge.getNode2();

    Graph graph = new Graph(tx);

    Set<String> otherUsers = graph.getNeighbors(EdgeType.HASHTAG, tagName, EdgeState.PROCESSED);

    if (otherUsers.contains(user)) {
      // this edge is already processed
      return;
    }

    HashSet<Edge> edges = new HashSet<>();
    for (String otherUser : otherUsers) {
      edges.add(new Edge(EdgeType.USER, user, otherUser));
    }

    Map<Edge, EdgeState> exsitingEdges = graph.getExistingEdges(edges);

    Map<Edge, EdgeState> processedEdges =
        Maps.filterValues(exsitingEdges, state -> state == EdgeState.PROCESSED);

    Tag tag = new Tag(tx, tagName);
    for (Edge newUserEdge : processedEdges.keySet()) {
      tag.queueNewUserEdge(newUserEdge);
    }

    tag.queueDegreeUpdate(user);

    tag.weaklyNotify();

    String updated = System.currentTimeMillis() + "";
    graph.setNodeAttribute(tagName, "updated", updated);
    graph.setNodeAttribute(user, "updated", updated);

    graph.setEdgeState(edge, EdgeState.PROCESSED);
  }

  public void processUserNewEdge(TransactionBase tx, Edge edge) {

    Graph graph = new Graph(tx);

    Set<String> tags1 = graph.getNeighbors(EdgeType.HASHTAG, edge.getNode1(), EdgeState.PROCESSED);
    Set<String> tags2 = graph.getNeighbors(EdgeType.HASHTAG, edge.getNode2(), EdgeState.PROCESSED);

    Set<String> tagsToIncrement = Sets.intersection(tags1, tags2);

    for (String tagName : tagsToIncrement) {
      Tag tag = new Tag(tx, tagName);
      tag.queueNewUserEdge(edge);
      tag.weaklyNotify();
    }

    graph.setEdgeState(edge, EdgeState.PROCESSED);

    String updated = System.currentTimeMillis() + "";
    graph.setNodeAttribute(edge.getNode1(), "updated", updated);
    graph.setNodeAttribute(edge.getNode2(), "updated", updated);
  }


  @Override
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
    Edge edge = Graph.decodeEdge(row, col);

    switch (edge.getType()) {
      case USER:
        processUserNewEdge(tx, edge);
        break;
      case HASHTAG:
        processNewHashtagEdge(tx, edge);
        break;
    }
  }


  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(Graph.EDGE_ADDTIME_COL, NotificationType.STRONG);
  }
}
