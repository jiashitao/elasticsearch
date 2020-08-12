package com.example.springbootelasticsearch.searchtest;

import com.example.springbootelasticsearch.utils.ESClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ESDocumentTest {
    //下面方法中需要用到的客户端类和序列化类
    RestHighLevelClient esClient = ESClient.getClient();
    ObjectMapper mapper = new ObjectMapper();

    @Test
    public void createDoc() throws IOException {
        //准备一个json数据
        Person person = new Person(1, "法外狂徒张三", 23, new Date());
        String json = mapper.writeValueAsString(person);
        //准备一个request对象
        IndexRequest request = new IndexRequest("person", "man", String.valueOf(person.getId()));
        request.source(json, XContentType.JSON);
        IndexResponse response = esClient.index(request, RequestOptions.DEFAULT);
        System.out.println(response.getResult());
    }

    @Test
    public void updateDoc() throws IOException {
        //使用doc修改的方式
        //创建一个Map，制定需要修改的内容
        Map<String, Object> map = new HashMap<>();
        map.put("name", "话说张三呐");
        //创建一个request对象，封装数据
        UpdateRequest request = new UpdateRequest("person", "man", "1");
        request.doc(map);
        //通过esClient对象执行
        UpdateResponse updateResponse = esClient.update(request, RequestOptions.DEFAULT);
        //输出返回结果
        System.out.println(updateResponse.getResult());
    }

    @Test
    public void deleteDoc() throws IOException {
        //创建一个request对象，封装数据
        DeleteRequest request = new DeleteRequest("person", "man", "1");
        //通过esClient对象执行
        DeleteResponse deleteResponse = esClient.delete(request, RequestOptions.DEFAULT);
        //输出返回结果
        System.out.println(deleteResponse.getResult());
    }

    @Test
    public void bulkCreateDoc() throws IOException {
        //准备多个json数据
        Person person1 = new Person(1, "法外狂徒张三", 23, new Date());
        Person person2 = new Person(2, "法外狂徒李四", 24, new Date());
        Person person3 = new Person(3, "法外狂徒王五", 25, new Date());
        //序列化操作
        String json1 = mapper.writeValueAsString(person1);
        String json2 = mapper.writeValueAsString(person2);
        String json3 = mapper.writeValueAsString(person3);
        //准备一个request对象
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("person", "man", String.valueOf(person1.getId())).source(json1, XContentType.JSON));
        bulkRequest.add(new IndexRequest("person", "man", String.valueOf(person2.getId())).source(json2, XContentType.JSON));
        bulkRequest.add(new IndexRequest("person", "man", String.valueOf(person3.getId())).source(json3, XContentType.JSON));
        //通过esClient执行操作
        BulkResponse bulkResponse = esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.status());
    }

    @Test
    public void bulkDeleteDoc() throws IOException {
        //准备一个request对象
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new DeleteRequest("person", "man", "1"));
        bulkRequest.add(new DeleteRequest("person", "man", "2"));
        bulkRequest.add(new DeleteRequest("person", "man", "3"));
        //通过esClient执行操作
        BulkResponse bulkResponse = esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.status());
    }
}
