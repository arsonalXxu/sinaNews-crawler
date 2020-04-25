package org.arsonal.sinaNewsCrawler;

import java.sql.SQLException;

public interface CrawlerDao {

    String getNextLinkThenDelete() throws SQLException;

    void insertLinkIntoDataBase(String currentLink) throws SQLException;

    boolean isLinkProcessed(String currentLink) throws SQLException;

    void insertNewsIntoDatabase(String title, String content, String url) throws SQLException;

    void insertLinkToBeProcessed(String link) throws SQLException;
}
