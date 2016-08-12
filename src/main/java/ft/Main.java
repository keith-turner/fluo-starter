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
import com.google.common.collect.Sets.SetView;
import ft.logic.HashTagEdgeObserver;
import ft.logic.NewEdgeObserver;
import ft.logic.TagObserver;
import ft.logic.TweetLoader;
import ft.logic.UserEdgeObserver;
import ft.model.Tweet;

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
  

  private static void preInit(FluoConfiguration fluoConfig) {
    fluoConfig.addObserver(new ObserverConfiguration(NewEdgeObserver.class.getName()));
    fluoConfig.addObserver(new ObserverConfiguration(TagObserver.class.getName()));
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
