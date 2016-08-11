package ft;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

public class Tweet {
  public final String from;
  public final List<String> hashtags;
  public final List<String> mentions;

  public Tweet(String from, List<String> hashtags, List<String> mentions) {
    this.from = from;
    this.hashtags = ImmutableList.copyOf(hashtags);
    // TODO remove self mentions
    this.mentions = ImmutableList.copyOf(mentions);
  }

  public static class TweetBuilder {
    private String from;
    private List<String> hashtags = Collections.emptyList();
    private List<String> mentions = Collections.emptyList();

    private TweetBuilder(String from) {
      this.from = from;
    }

    public Tweet.TweetBuilder withTags(String... tags) {
      this.hashtags = Arrays.asList(tags);
      return this;
    }

    public Tweet.TweetBuilder withMentions(String... mentions) {
      this.mentions = Arrays.asList(mentions);
      return this;
    }

    public Tweet build() {
      return new Tweet(from, hashtags, mentions);
    }
  }

  public static Tweet.TweetBuilder newTweet(String from) {
    return new TweetBuilder(from);
  }
}