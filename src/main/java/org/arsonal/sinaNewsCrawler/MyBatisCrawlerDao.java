package org.arsonal.sinaNewsCrawler;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class MyBatisCrawlerDao implements CrawlerDao {
    private SqlSessionFactory sqlSessionFactory;

    public MyBatisCrawlerDao() {
        String resource = "db/mybatis/config.xml";
        try {
            InputStream inputStream = Resources.getResourceAsStream(resource);
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized String getNextLinkThenDelete() {
        String link;
        try (SqlSession session = sqlSessionFactory.openSession(true)) {
            link = session.selectOne("org.arsonal.MyMapper.getLink");
            if (link != null) {
                session.delete("org.arsonal.MyMapper.deleteLink", link);
            }
        }
        return link;
    }

    @Override
    public boolean isLinkProcessed(String link) {
        int count;
        try (SqlSession session = sqlSessionFactory.openSession()) {
            count = session.selectOne("org.arsonal.MyMapper.countLink", link);
        }
        return count != 0;
    }

    @Override
    public void insertNewsIntoDatabase(String title, String content, String url) {
        try (SqlSession session = sqlSessionFactory.openSession(true)) {
            session.insert("org.arsonal.MyMapper.insertNews", new News(title, content, url));
        }
    }

    @Override
    public void insertLinkIntoDataBase(String link) {
        Map<String, Object> param = new HashMap<>();
        param.put("tableName", "LINKS_ALREADY_PROCESSED");
        param.put("link", link);

        try (SqlSession session = sqlSessionFactory.openSession(true)) {
            session.insert("org.arsonal.MyMapper.insertLink", param);
        }
    }

    @Override
    public void insertLinkToBeProcessed(String link) {
        Map<String, Object> param = new HashMap<>();
        param.put("tableName", "LINKS_TO_BE_PROCESSED");
        param.put("link", link);

        try (SqlSession session = sqlSessionFactory.openSession(true)) {
            session.insert("org.arsonal.MyMapper.insertLink", param);
        }
    }
}
