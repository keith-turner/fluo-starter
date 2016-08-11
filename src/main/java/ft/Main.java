package ft;

import java.io.File;
import java.nio.file.Files;
import java.util.*;

// Normaly using * with imports is a bad practice, however in this case it makes experimenting with
// Fluo easier.
import org.apache.fluo.api.client.*;
import org.apache.fluo.api.config.*;
import org.apache.fluo.api.data.*;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.api.observer.*;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

public class Main {
  public static void main(String[] args) throws Exception {

    String tmpDir = Files.createTempDirectory(new File("target").toPath(), "mini").toString();
    // System.out.println("tmp dir : "+tmpDir);

    FluoConfiguration fluoConfig = new FluoConfiguration();
    fluoConfig.setApplicationName("class");
    fluoConfig.setMiniDataDir(tmpDir);

    preInit(fluoConfig);

    System.out.print("Starting MiniFluo ... ");

    try (MiniFluo mini = FluoFactory.newMiniFluo(fluoConfig);
        FluoClient client = FluoFactory.newClient(mini.getClientConfiguration())) {

      System.out.println("started.");

      excercise(mini, client);
    }
  }

  static final Tweet[] tweets = new Tweet[] {Tweet.newTweet("@user1").withTags("#tag1").build(),
      Tweet.newTweet("@user1").withTags("#tag2").build(),
      Tweet.newTweet("@user1").withTags("#tag3").withMentions("@user2").build(),
      Tweet.newTweet("@user2").withTags("#tag1").build(),
      Tweet.newTweet("@user3").withTags("#tag1").build(),
      Tweet.newTweet("@user4").withMentions("@user1", "@user6").build(),
      Tweet.newTweet("@user4").withTags("#tag2").build(),
      Tweet.newTweet("@user5").withTags("#tag2").withMentions("@user1").build(),
      Tweet.newTweet("@user6").withTags("#tag2", "#tag3").withMentions("@user7").build(),};
  
  public static final Column HASHTAG_EDGE_COL = new Column("edge", "hashtag");
  public static final Column USER_EDGE_COL = new Column("edge", "user");

  public static final String HASHTAG_EDGE = "htag";
  public static final String USER_EDGE = "user";
  

  public static class HashTagEdgeObserver extends NewEdgeObserver {

    public HashTagEdgeObserver() {
      super(HASHTAG_EDGE);
    }

    @Override
    public void processNewEdge(TransactionBase tx, Edge edge) {
      
      String tag = edge.getNode1();
      String user = edge.getNode2();
      
      Graph graph = new Graph(tx);
      
      Set<String> otherUsers = graph.getNeighbors(HASHTAG_EDGE, tag);
      
      
      HashSet<Edge> edges = new HashSet<>();
      for (String otherUser : otherUsers) {
        edges.add(new Edge(USER_EDGE, user, otherUser));
      }
      
      Set<Edge> newUserEdges = graph.getProcessedEdges(edges);
      for(Edge newUserEdge : newUserEdges) {
        TagPersistence.newUserEdge(tag, newUserEdge);
      }
      
      TagPersistence.incrementDegree(tag);
      graph.lockNode(user);
    }



  }

  public static class UserEdgeObserver extends NewEdgeObserver {

    public UserEdgeObserver() {
      super(USER_EDGE);
    }

    @Override
    public void processNewEdge(TransactionBase tx, Edge edge) {
      
      Graph graph = new Graph(tx);
      
      Set<String> tags1 = graph.getNeighbors(HASHTAG_EDGE, edge.getNode1());
      Set<String> tags2 = graph.getNeighbors(HASHTAG_EDGE, edge.getNode2());
      
      Set<String> tagsToIncrement = Sets.intersection(tags1, tags2);
      
      for (String tag : tagsToIncrement) {
        TagPersistence.newUserEdge(tag, edge);
      }
      
      graph.lockNode(edge.getNode1());
      graph.lockNode(edge.getNode2());
    }

  }

  public static class TweetLoader implements Loader {

    private Tweet tweet;

    public TweetLoader(Tweet tweet) {
      this.tweet = tweet;
    }

    @Override
    public void load(TransactionBase tx, Context context) throws Exception {
   
      Set<Edge> tweetEdges = new HashSet<>();
      
      for (String hashtag : tweet.hashtags) {
        tweetEdges.add(new Edge(HASHTAG_EDGE, tweet.from, hashtag));
      }

      for (String mention : tweet.mentions) {
        tweetEdges.add(new Edge(USER_EDGE, mention, tweet.from));
      }

      Graph graph = new Graph(tx);
      
      Set<Edge> existing = graph.getExistingEdges(tweetEdges);
      Set<Edge> edgesToAdd = Sets.difference(tweetEdges, existing);
      
      graph.addEdges(tweetEdges);
    }
  }

  private static void preInit(FluoConfiguration fluoConfig) {
    fluoConfig.addObserver(new ObserverConfiguration(HashTagEdgeObserver.class.getName()));
    fluoConfig.addObserver(new ObserverConfiguration(UserEdgeObserver.class.getName()));
  }

  private static void excercise(MiniFluo mini, FluoClient client) {
    try (LoaderExecutor loader = client.newLoaderExecutor()) {
      for (Tweet tweet : tweets) {
        loader.execute(new TweetLoader(tweet));
      }
    }

    mini.waitForObservers();

    try (Snapshot snap = client.newSnapshot()) {
      snap.scanner().build().forEach(System.out::println);
    }
    // TODO scan table an print out clustering coefficients
  }
}
