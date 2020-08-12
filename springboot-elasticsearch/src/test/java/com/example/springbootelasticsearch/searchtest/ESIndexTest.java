package com.example.springbootelasticsearch.searchtest;

import com.example.springbootelasticsearch.utils.ESClient;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

public class ESIndexTest {

    RestHighLevelClient esClient = ESClient.getClient();

    @Test
    public void createIndex() throws IOException {
        //准备关于索引的settings
        Settings.Builder settings = Settings.builder()
                .put("number_of_shards", 5)
                .put("number_of_replicas", 1);

        //准备关于索引的结构mappings
        XContentBuilder mappings = JsonXContent.contentBuilder()
                .startObject()
                .startObject("properties")
                .startObject("name")
                .field("type", "text")
                .endObject()
                .startObject("age")
                .field("type", "integer")
                .endObject()
                .startObject("birthday")
                .field("type", "date")
                .field("format", "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis")
                .endObject()
                .endObject()
                .endObject();
//        将settings和mappings封装到一个CreateIndexRequest对象中
        CreateIndexRequest request = new CreateIndexRequest("person")
                .settings(settings)
                .mapping("man", mappings);
        //通过client对象去连接ES并执行创建索引
        esClient.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    public void existsIndex() throws IOException {
//        准备Request对象
        GetIndexRequest request = new GetIndexRequest();
        request.indices("person");
        //通过client对象去连接ES并执行
        boolean exists = esClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    @Test
    public void deleteIndex() throws IOException {
        //准备Request对象
        DeleteIndexRequest request = new DeleteIndexRequest("person");
        //通过client对象去连接ES并执行
        AcknowledgedResponse response = esClient.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(response.isAcknowledged());
    }
}
