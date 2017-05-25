package webcrawler;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements; 
/**
 *
 * @author Mark
 */
public class WebCrawler implements Runnable{
    static Object lock = new Object();
    private Document doc;
    private String html;
    private final Pattern regex = Pattern.compile("[A-Za-z0-9._%+-]+@(?:[A-Za-z0-9-]+\\.)+[A-Za-z]{2,4}");
    private Matcher matcher;
    static HashSet emails;
    private String currentURL;
    static List <String> links;
    static boolean finished;
    static Set<String> visitedLinks;
    int page;
    static int urlIndex;
    
    public WebCrawler(HashSet emails, List links) throws IOException {
        this.emails = emails;
        this.links =  links;
        visitedLinks = Collections.synchronizedSet(new HashSet<>());
        currentURL = "https://www.touro.edu/";
    }
    @Override
    public void run() {
        while (emails.size() != 1000) {
            findUnvisitedLink();
            parseHTML();
            findEmails();
            collectHyperLinks();
        } 
        finished = true;
    }
    
    private void findUnvisitedLink() {
        synchronized(lock) {
            while (visitedLinks.contains(currentURL)) {
                while (urlIndex == links.size()) {
                    try {
                        lock.wait(1000);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                currentURL = links.get(urlIndex);
                urlIndex++;
            }
            addVisitedLink(currentURL);
            try {
                doc = Jsoup.connect(currentURL).get();
            } catch (IOException ex) {
                Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    private void addVisitedLink(String link) {
        visitedLinks.add(link);
    }

    private void parseHTML() {
        html = doc.html();
    }
    
    private void findEmails() {
        page++;
        matcher = regex.matcher(html);
        synchronized(lock) {
            while (matcher.find()) {
                if (emails.size() == 1000) {
                    break;
                }
                emails.add(matcher.group());
                System.out.println(emails.size());
            } 
        }
    }
   
    private void collectHyperLinks() {
       Elements Hyperlinks = doc.select("a[href]");
       for (Element link : Hyperlinks) {
           String potentialLink = "" + link.attr("abs:href");
           if (!links.contains(potentialLink) && linkNotBlocked(potentialLink)) {
               links.add(potentialLink);
           }
       }
    }
    
    private boolean linkNotBlocked(String link) {
        boolean validLink = false;
        if (!link.contains("twitter") && !link.contains("youtube")  && !link.contains("vimeo")) {
            validLink = true;
        }
        return validLink;           
    }
    
    public static void main(String [] args) throws IOException, SQLException {
        HashSet email = new HashSet<>();
        ArrayList<String> foundEmails = new ArrayList<>();
        EmailDataBase db = new EmailDataBase();
        List listOfLinks = Collections.synchronizedList(new ArrayList<String>());
        for (int i = 0; i < 100; i++) {
            WebCrawler crawler = new WebCrawler(email, listOfLinks);
            Thread t1 = new Thread(crawler);
            t1.start();
        }
        while (!finished) {}
        System.out.println("these are the emails that have been found");
        for (Object mail: email) {
            System.out.println(mail);
            foundEmails.add((String) mail);
        }
        db.checkAndUpdateEmails(foundEmails);
        System.exit(0);
    }
}
