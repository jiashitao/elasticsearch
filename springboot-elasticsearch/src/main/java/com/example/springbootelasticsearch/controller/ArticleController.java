package com.example.springbootelasticsearch.controller;

import com.example.springbootelasticsearch.entity.Article;
import com.example.springbootelasticsearch.repository.ArticleRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

@Slf4j
@RestController
public class ArticleController {
    @Autowired
    private ArticleRepository articleRepository;

    @GetMapping("/search/{keyword}")
    public List<Article> getInfo(HttpServletRequest request, @PathVariable("keyword") String keyword) {
        log.info(request.getRequestURI());
        log.info("搜索的关键字为：{}", keyword);
        Pageable pageable = PageRequest.of(0, 30);
        try {
            List<Article> articleList = articleRepository.findAllByTitleOrAuthorOrContent(keyword, keyword, keyword, pageable);
            return articleList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
