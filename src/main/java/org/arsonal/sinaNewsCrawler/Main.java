package org.arsonal.sinaNewsCrawler;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Main {
    // 从数据库加载即将要处理的链接的代码
    static String driver = "org.h2.Driver";
    static String url = "jdbc:h2:D:/development/tmp/sinaNews-crawler/news";
    static final String USER = "root";
    static final String PASSWORD = "root";

    @SuppressFBWarnings("DMI_CONSTANT_DB_PASSWORD")
    public static void main(String[] args) throws IOException, SQLException {

        Connection connection = DriverManager.getConnection(url, USER, PASSWORD);

        // 循环处理连接池
        while (true) {
            List<String> linkPool = loadUrlFromDataBase(connection, "select * from LINKS_TO_BE_PROCESSED");
            if (linkPool.isEmpty()) {
                break;
            }
            // ArrayLis从尾部删除更有效率
            String currentLink = linkPool.remove(linkPool.size() - 1);
            deleteLinkFromDataBase(connection, currentLink, "delete from LINKS_TO_BE_PROCESSED where LINK = ?");
            if (isLinkProcessed(connection, currentLink)) {
                continue;
            }

            if (isInterestedLink(currentLink)) {

                currentLink = convertURL(currentLink);
                System.out.println("当前链接：" + currentLink);

                Document doc = httpGetAndParseHtml(currentLink);

                Elements links = doc.getElementsByTag("a");
                parseUrlIfItNewsInsertIntoDataBase(connection, links);

                storeIntoDBIfItIsNewsPage(currentLink, doc, connection);
            } else {
                insertLinkIntoDataBase(connection, currentLink, "insert into LINKS_ALREADY_PROCESSED(LINK) values ( ? )");
            }
        }

    }

    private static void parseUrlIfItNewsInsertIntoDataBase(Connection connection, Elements links) throws SQLException {
        for (Element aTag : links) {
            String href = aTag.attr("href");
            insertLinkIntoDataBase(connection, href, "insert into LINKS_TO_BE_PROCESSED(LINK) values(?)");
        }
    }

    private static void deleteLinkFromDataBase(Connection connection, String link, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, link);
            statement.executeUpdate();
        }
    }

    private static boolean isLinkProcessed(Connection connection, String currentLink) throws SQLException {
        ResultSet resultSet = null;
        try (PreparedStatement statement = connection.prepareStatement("select LINK from LINKS_ALREADY_PROCESSED where LINK = ?")) {
            statement.setString(1, currentLink);
            resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return true;
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return false;
    }

    private static void insertLinkIntoDataBase(Connection connection, String currentLink, String s) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(s)) {
            statement.setString(1, currentLink);
            statement.executeUpdate();
        }
    }

    private static List<String> loadUrlFromDataBase(Connection connection, String sql) throws SQLException {
        List<String> list = new ArrayList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                list.add(resultSet.getString(1));
            }
        }
        return list;
    }

    private static void storeIntoDBIfItIsNewsPage(String currentLink, Document doc, Connection connection) throws SQLException {
        Elements articles = doc.getElementsByTag("article");
        if (articles.isEmpty()) {
            insertLinkIntoDataBase(connection, currentLink, "insert into LINKS_ALREADY_PROCESSED(LINK) values ( ? )");
        } else {
            for (Element article : articles) {
                // TODO
                System.out.println(article.getElementsByTag("h1").text());

            }
        }
        insertLinkIntoDataBase(connection, currentLink, "insert into LINKS_ALREADY_PROCESSED(LINK) values ( ? )");
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
