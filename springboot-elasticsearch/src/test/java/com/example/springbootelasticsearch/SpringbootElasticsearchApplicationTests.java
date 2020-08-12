package com.example.springbootelasticsearch;

import com.example.springbootelasticsearch.entity.Article;
import com.example.springbootelasticsearch.mapper.ArticleMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.document.Document;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Date;

@SpringBootTest
class SpringbootElasticsearchApplicationTests {
    @Autowired
    private ElasticsearchRestTemplate template;
    @Resource
    private ArticleMapper articleMapper;

    @Test
    void contextLoads() {
        /*template.indexOps(Article.class).delete();
        template.indexOps(Article.class).create();*/
        Document document = template.indexOps(Article.class).createMapping();
        template.indexOps(Article.class).putMapping(document);
    }

    @Test
    void testSave() {
        ArrayList<Article> arrayList = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Article article = new Article();
            article.setId((long) i);
            article.setTitle("中共中央政治局召开会议 决定召开十九届五中全会 分析研究当前经济形势和经济工作 中共中央总书记习近平主持会议" + i);
            article.setAuthor("新华社" + i);
            article.setContent("新华社北京7月30日电中共中央政治局7月30日召开会议，决定今年10月在北京召开中国共产党第十九届中央委员会第五次全体会议，主要议程是，中共中央政治局向中央委员会报告工作，研究关于制定国民经济和社会发展第十四个五年规划和二〇三五年远景目标的建议。会议分析研究当前经济形势，部署下半年经济工作。中共中央总书记习近平主持会议。" + i);
            article.setDate(new Date());
            article.setStatus(1);
            arrayList.add(article);
        }
        template.save(arrayList);
    }

    @Test
    void testMybatisInsert() {
        for (int i = 30; i < 40; i++) {
            Article article = new Article();
            article.setId((long) i);
            article.setTitle("柯达改行制药！胶卷变胶囊，也是专业对口？" + i);
            article.setAuthor("中国新闻网" + i);
            article.setContent("中新网客户端北京7月31日电 (彭婧如)30日，“柯达一夜熔断13次”登上微博热搜榜。\n" +
                    "\n" +
                    "网友：生产胶卷的那个柯达吗？还以为是同名。\n" +
                    "\n" +
                    "柯达：不是同名，是我本尊。\n" +
                    "\n" +
                    "柯达改行制药！胶卷变胶囊，也是专业对口？\n" +
                    "2009年6月24日，柯达公司宣布于2009年停止生产拥有74年历史的柯达彩色胶片。\n" +
                    "\n" +
                    "柯达改行制药，股价暴涨频频熔断。" + i);
            article.setDate(new Date());
            article.setStatus(1);
            articleMapper.save(article);
        }
    }

}
