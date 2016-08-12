package ft.logic;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import ft.model.Edge;
import ft.model.EdgeState;
import ft.model.EdgeType;
import ft.model.Tweet;
import ft.persistence.Graph;
import org.apache.fluo.api.client.Loader;
import org.apache.fluo.api.client.TransactionBase;

public class TweetLoader implements Loader {

  private Tweet tweet;

  public TweetLoader(Tweet tweet) {
    this.tweet = tweet;
  }

  @Override
  public void load(TransactionBase tx, Context context) throws Exception {

    Set<Edge> tweetEdges = new HashSet<>();
    
    for (String hashtag : tweet.hashtags) {
      tweetEdges.add(new Edge(EdgeType.HASHTAG, tweet.from, hashtag));
    }

    for (String mention : tweet.mentions) {
      if(!tweet.from.equals(mention)) {
        tweetEdges.add(new Edge(EdgeType.USER, mention, tweet.from));
      }
    }

    Graph graph = new Graph(tx);
    
    Map<Edge, EdgeState> existing = graph.getExistingEdges(tweetEdges);
    Set<Edge> edgesToAdd = Sets.difference(tweetEdges, existing.keySet());
    graph.addNewEdges(edgesToAdd);
  }
}