package com.example.springbootelasticsearch.mapper;

import com.example.springbootelasticsearch.entity.Article;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface ArticleMapper {
    @Insert("insert into article values(#{article.id},#{article.title},#{article.author},#{article.content},#{article.date},#{article.status})")
    @Options(keyProperty = "article.id", useGeneratedKeys = true)
    public int save(@Param("article") Article article);
}
