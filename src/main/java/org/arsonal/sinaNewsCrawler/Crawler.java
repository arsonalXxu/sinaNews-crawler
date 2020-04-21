package org.arsonal.sinaNewsCrawler;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.sql.SQLException;
import java.util.stream.Collectors;

public class Crawler {
    private JDBCCrawlerDao dao = new JDBCCrawlerDao();

    public static void main(String[] args) {
        Crawler crawler = new Crawler();
        try {
            crawler.run();
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public void run() throws IOException, SQLException {

        String currentLink;
        while ((currentLink = dao.getNextLinkThenDelete("select * from LINKS_TO_BE_PROCESSED limit 1")) != null) {
            if (dao.isLinkProcessed(currentLink)) {
                continue;
            }
            if (isInterestedLink(currentLink)) {

                currentLink = convertURL(currentLink);
                System.out.println("当前链接：" + currentLink);

                Document doc = httpGetAndParseHtml(currentLink);

                Elements links = doc.getElementsByTag("a");
                parseUrlIfItNewsInsertIntoDataBase(links);

                storeIntoDBIfItIsNewsPage(currentLink, doc);
            }
        }

    }

    private void parseUrlIfItNewsInsertIntoDataBase(Elements links) throws SQLException {
        for (Element aTag : links) {
            String href = aTag.attr("href");
            dao.insertLinkIntoDataBase(href, "insert into LINKS_TO_BE_PROCESSED(LINK) values(?)");
        }
    }


    private void storeIntoDBIfItIsNewsPage(String currentLink, Document doc) throws SQLException {
        Elements articles = doc.getElementsByTag("article");
        if (articles.isEmpty()) {
            dao.insertLinkIntoDataBase(currentLink, "insert into LINKS_ALREADY_PROCESSED(LINK) values ( ? )");
        } else {
            for (Element article : articles) {
                // TODO
                System.out.println(article.getElementsByTag("h1").text());
                String title = article.getElementsByTag("h1").text();
                String content = article.getElementsByTag("p").stream().filter((element) -> element.select("art_p") != null)
                        .map(Element::text).collect(Collectors.joining("\n"));

                dao.insertNewsIntoDatabase(title, content, currentLink);
            }
        }
        dao.insertLinkIntoDataBase(currentLink, "insert into LINKS_ALREADY_PROCESSED(LINK) values ( ? )");
    }

    private static Document httpGetAndParseHtml(String link) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(link);
        try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
            HttpEntity entity1 = response.getEntity();
            return Jsoup.parse(EntityUtils.toString(entity1));
        }
    }

    private static boolean isInterestedLink(String currentLink) {
        return isIndexPage(currentLink, "https://sina.cn") && !isLoginPage(currentLink) || startWithNewsPage(currentLink);
    }

    private static boolean isIndexPage(String currentLink, String s) {
        return currentLink.contains(s);
    }

    private static boolean isLoginPage(String currentLink) {
        return isIndexPage(currentLink, "passport.sina.cn");
    }

    private static boolean startWithNewsPage(String currentLink) {
        return currentLink.contains("news.sina.cn");
    }

    public static String convertURL(String link) {

        String url = null;
        if (link.startsWith("//")) {
            link = "https:" + link;
        }
        try {
            url = link.trim().replace(" ", "%20")
                    .replace("|", "%7C");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return url;
    }
}
