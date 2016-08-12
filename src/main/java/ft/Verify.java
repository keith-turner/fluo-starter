package ft;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import ft.model.Tweet;

public class Verify {
  
  private static String ne(String u1, String u2) {
    int cmp = u1.compareTo(u2);
    if(cmp < 0)
      return u1+":"+u2;
    else if(cmp > 0)
      return u2+":"+u1;
    
    throw new IllegalArgumentException();
  }
  
  
  Map<String, Set<String>> tags = new TreeMap<>();
  Set<String> edges = new HashSet<>();
  
  public void add(Tweet ... tweets){
    
    for (Tweet tweet : tweets) {
      for(String tag : tweet.hashtags){
        tags.computeIfAbsent(tag, k -> new HashSet<>()).add(tweet.from);
      }
      
      for(String mentioned : tweet.mentions) {
        edges.add(ne(tweet.from, mentioned));
      }
    }
  }
  
  public void print() {
    for(String tag : tags.keySet()) {
      
      int links = 0;
      
      Set<String> users = tags.get(tag);
      for (String u1 : users) {
        for (String u2 : users) {
          if(u1.equals(u2)){
            continue;
          }
          
          if(edges.contains(ne(u1,u2)) && u1.compareTo(u2) > 0) {
            links++;
          }
        }
      }
    
      double degree = users.size(); 
      double cc = (double)links / (degree * (degree -1 )/2); 
      System.out.println("V "+tag +" "+cc);
      
      //System.out.println("V "+tag + " " + users);
      
    }
    
    //System.out.println("V "+edges);
  }
}
