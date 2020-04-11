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
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Main {
    public static void main(String[] args) throws IOException {
        // 待处理的链接池
        List<String> linkPool = new ArrayList<>();
        // 已处理的连接池
        Set<String> handledLinks = new HashSet<>();

        String sinaNews = "https://sina.cn";
        linkPool.add(sinaNews);

        // 循环处理连接池
        while (true) {
            if (linkPool.isEmpty()) {
                break;
            }

            String currentLink = linkPool.remove(linkPool.size() - 1);
            if (handledLinks.contains(currentLink)) {
                continue;
            }

            if (isInterestedLink(currentLink)) {
                if (currentLink.startsWith("//")) {
                    currentLink = "https:" + currentLink;
                }
                System.out.println("当前链接：" + currentLink);
                currentLink = convertURL(currentLink);

                Document doc = httpGetAndParseHtml(currentLink);

                Elements links = doc.getElementsByTag("a");
                links.stream().map(aTag -> aTag.attr("href")).forEach(linkPool::add);

                storeIntoDBIfItIsNewsPage(handledLinks, currentLink, doc);
            } else {
                handledLinks.add(currentLink);
            }
        }

    }

    private static void storeIntoDBIfItIsNewsPage(Set<String> handledLinks, String currentLink, Document doc) {
        Elements articles = doc.getElementsByTag("article");
        if (articles.isEmpty()) {
            handledLinks.add(currentLink);
        } else {
            for (Element article : articles) {
                // TODO
                System.out.println(article.text());
                System.out.println("123");
            }
        }
    }

    private static Document httpGetAndParseHtml(String link) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(link);
        try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
            System.out.println(response.getStatusLine());
            HttpEntity entity1 = response.getEntity();
            return Jsoup.parse(EntityUtils.toString(entity1));
        }
    }

    private static boolean isInterestedLink(String currentLink) {
        return isIndexPage(currentLink, "sina.cn") && !isLoginPage(currentLink);
    }

    private static boolean isIndexPage(String currentLink, String s) {
        return currentLink.contains(s);
    }

    private static boolean isLoginPage(String currentLink) {
        return isIndexPage(currentLink, "passport.sina.cn");
    }

    public static String convertURL(String str) {

        String url = null;
        try {
            url = new String(str.trim().replace(" ", "%20")
                    .replace("|", "%7C"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return url;
    }
}
