import org.jsoup.Jsoup;
import org.jsoup.helper.Validate;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;



/**
 * Created by yjl
 * Data: 2019/3/17
 * Time: 16:25
 */
public class FlyCatJsoup {
    public static void main(String[] args) throws Exception {
        String url = "https://book.jd.com/";
        Document doc = Jsoup.connect(url).get();
        Elements links = doc.select("p[href]");
        Elements body = doc.select("body");



    }
}
