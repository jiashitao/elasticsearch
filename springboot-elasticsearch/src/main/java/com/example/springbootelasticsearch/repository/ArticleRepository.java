package com.example.springbootelasticsearch.repository;

import com.example.springbootelasticsearch.entity.Article;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface ArticleRepository extends ElasticsearchRepository<Article, Long> {
    public List<Article> findAllByTitleOrAuthorOrContent(String title, String author, String content, Pageable pageable);
}
