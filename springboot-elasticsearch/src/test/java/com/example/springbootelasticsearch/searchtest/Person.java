package com.example.springbootelasticsearch.searchtest;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
public class Person {
    @JsonIgnore
    private int id;
    private String name;
    private int age;
    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date birthday;
}
