package org.arsonal.sinaNewsCrawler;

import java.sql.SQLException;

public interface CrawlerDao {
    String getNextLinkFromDataBase(String sql) throws SQLException;

    String getNextLinkThenDelete(String sql) throws SQLException;

    void deleteLinkFromDataBase(String link, String sql) throws SQLException;

    void insertLinkIntoDataBase(String currentLink, String s) throws SQLException;

    boolean isLinkProcessed(String currentLink) throws SQLException;

    void insertNewsIntoDatabase(String title, String content, String url) throws SQLException;
}
