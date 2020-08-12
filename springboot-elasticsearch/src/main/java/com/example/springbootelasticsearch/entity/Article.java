package com.example.springbootelasticsearch.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.Date;

@Data
@Document(indexName = "article_index")
public class Article {
    @Id
    @Field(type = FieldType.Long, store = true)
    private Long id;
    @Field(type = FieldType.Text, store = true, analyzer = "ik_smart")
    public String title;
    @Field(type = FieldType.Text, store = true, analyzer = "ik_smart")
    public String author;
    @Field(type = FieldType.Text, store = true, analyzer = "ik_smart")
    public String content;
    @Field(type = FieldType.Date, format = DateFormat.date_optional_time)
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    public Date date;
    @Field(type = FieldType.Integer, store = true)
    public Integer status;


}
