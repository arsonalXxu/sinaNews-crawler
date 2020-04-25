package org.arsonal.sinaNewsCrawler;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JDBCCrawlerDao implements CrawlerDao {
//    static String driver = "org.h2.Driver";
    static String url = "jdbc:h2:D:/development/tmp/sinaNews-crawler/news";
    static final String USER_NAME = "root";
    static final String PASSWORD = "root";
    private final Connection connection;

    @SuppressFBWarnings("DMI_CONSTANT_DB_PASSWORD")
    public JDBCCrawlerDao() {
        try {
            this.connection = DriverManager.getConnection(url, USER_NAME, PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertNewsIntoDatabase(String title, String content, String url) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("insert into NEWS(title, content, url, create_at, modified_at) VALUES ( ?,?,?,now(),now())")) {
            statement.setString(1, title);
            statement.setString(2, content);
            statement.setString(3, url);
            statement.executeUpdate();
        }
    }

    @Override
    public void insertLinkToBeProcessed(String link) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("insert into LINKS_TO_BE_PROCESSED(LINK) values ( ? )")) {
            statement.setString(1, link);
            statement.executeUpdate();
        }
    }

    private String getNextLinkFromDataBase() throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement("select * from LINKS_TO_BE_PROCESSED limit 1");
             ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                return resultSet.getString(1);
            }
        }
        return null;
    }

    @Override
    public String getNextLinkThenDelete() throws SQLException {
        String link = getNextLinkFromDataBase();
        if (link != null) {
            deleteLinkFromDataBase(link, "delete from LINKS_TO_BE_PROCESSED where LINK = ?");
        }
        return link;
    }

    private void deleteLinkFromDataBase(String link, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, link);
            statement.executeUpdate();
        }
    }

    @Override
    public void insertLinkIntoDataBase(String currentLink) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("insert into LINKS_ALREADY_PROCESSED(LINK) values ( ? )")) {
            statement.setString(1, currentLink);
            statement.executeUpdate();
        }
    }

    @Override
    public boolean isLinkProcessed(String currentLink) throws SQLException {
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
}
