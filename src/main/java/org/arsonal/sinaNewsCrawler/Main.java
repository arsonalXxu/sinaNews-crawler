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
import java.util.stream.Collectors;

public class Main {
    // 从数据库加载即将要处理的链接的代码
    static String driver = "org.h2.Driver";
    static String url = "jdbc:h2:D:/development/tmp/sinaNews-crawler/news";
    static final String USER = "root";
    static final String PASSWORD = "root";

    @SuppressFBWarnings("DMI_CONSTANT_DB_PASSWORD")
    public static void main(String[] args) throws IOException, SQLException {

        Connection connection = DriverManager.getConnection(url, USER, PASSWORD);

        String currentLink;
        // 循环处理连接池
        while ((currentLink = getNextLinkThenDelete(connection, "select * from LINKS_TO_BE_PROCESSED limit 1")) != null) {
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

    private static String getNextLinkFromDataBase(Connection connection, String sql) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                return resultSet.getString(1);
            }
        }
        return null;
    }

    private static String getNextLinkThenDelete(Connection connection, String sql) throws SQLException {
        String link = getNextLinkFromDataBase(connection, sql);
        if (link != null) {
            deleteLinkFromDataBase(connection, link, "delete from LINKS_TO_BE_PROCESSED where LINK = ?");
        }
        return link;
    }

    private static void storeIntoDBIfItIsNewsPage(String currentLink, Document doc, Connection connection) throws SQLException {
        Elements articles = doc.getElementsByTag("article");
        if (articles.isEmpty()) {
            insertLinkIntoDataBase(connection, currentLink, "insert into LINKS_ALREADY_PROCESSED(LINK) values ( ? )");
        } else {
            for (Element article : articles) {
                // TODO
                System.out.println(article.getElementsByTag("h1").text());
                String title = article.getElementsByTag("h1").text();
                String content = article.getElementsByTag("p").stream().filter((element) -> element.select("art_p") != null)
                        .map(Element::text).collect(Collectors.joining("\n"));

                try (PreparedStatement statement = connection.prepareStatement("insert into NEWS(title, content, url, create_at, modified_at) VALUES ( ?,?,?,now(),now())")) {
                    statement.setString(1, title);
                    statement.setString(2, content);
                    statement.setString(3, currentLink);
                    statement.executeUpdate();
                }
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
