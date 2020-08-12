package com.example.springbootelasticsearch.searchtest;

import com.example.springbootelasticsearch.utils.ESClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ESSearchTest {

    //下面方法中需要用到的客户端类和序列化类
    RestHighLevelClient esClient = ESClient.getClient();
    ObjectMapper mapper = new ObjectMapper();
    String index = "sms-logs-index";

    @Test
    public void testTermQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("province", "北京"));
        sourceBuilder.from(0);
        sourceBuilder.size(5);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }
    @Test
    public void testTermsQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termsQuery("province", "北京", "山西"));
        sourceBuilder.from(0);
        sourceBuilder.size(5);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    @Test
    public void testMatchAllQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.size(20);//es不指定的情况下默认查询10条数据
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
        System.out.println(response.getHits().getTotalHits());
    }

    @Test
    public void testMatchQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("smsContent", "收货安装"));
        sourceBuilder.size(20);//es不指定的情况下默认查询10条数据
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
        System.out.println(response.getHits().getTotalHits());
    }

    @Test
    public void testBoolMatchQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("smsContent", "中国健康").operator(Operator.AND));
        sourceBuilder.size(20);//es不指定的情况下默认查询10条数据
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
        System.out.println(response.getHits().getTotalHits());
    }

    @Test
    public void testMultiMatchQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.multiMatchQuery("北京", "province", "smsContent"));
        sourceBuilder.size(20);//es不指定的情况下默认查询10条数据
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
        System.out.println(response.getHits().getTotalHits());
    }

    @Test
    public void testGetById() throws IOException {
        //创建Request对象
        GetRequest request = new GetRequest(index);
        request.id("21");

        //执行查询
        GetResponse response = esClient.get(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        System.out.println(response.getSourceAsString());
    }

    @Test
    public void testIdsQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.idsQuery().addIds("21", "22", "23"));
        sourceBuilder.size(20);//es不指定的情况下默认查询10条数据
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
        System.out.println(response.getHits().getTotalHits());
    }

    @Test
    public void testPrefixQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.prefixQuery("corpName", "途虎"));
        sourceBuilder.size(20);//es不指定的情况下默认查询10条数据
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
        System.out.println(response.getHits().getTotalHits());
    }

    @Test
    public void testFuzzyQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.fuzzyQuery("corpName", "盒马先生").prefixLength(2));
        sourceBuilder.size(20);//es不指定的情况下默认查询10条数据
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
        System.out.println(response.getHits().getTotalHits());
    }

    @Test
    public void testWildcardQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.wildcardQuery("corpName", "中国*"));
        sourceBuilder.size(20);//es不指定的情况下默认查询10条数据
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
        System.out.println(response.getHits().getTotalHits());
    }

    @Test
    public void testRangeQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.rangeQuery("fee").gte(5).lte(10));
        sourceBuilder.size(20);//es不指定的情况下默认查询10条数据
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
        System.out.println(response.getHits().getTotalHits());
    }

    @Test
    public void testRegexpQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.regexpQuery("mobile", "139[0-9]{8}"));
        sourceBuilder.size(20);//es不指定的情况下默认查询10条数据
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
        System.out.println(response.getHits().getTotalHits());
    }
    @Test
    public void testScrollQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定Scroll信息,这里主要指生存时间
        request.scroll(TimeValue.timeValueMillis(1L));

        //指定查询条件，size，排序规则
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(5);
        sourceBuilder.sort("fee", SortOrder.DESC);
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//        获取ScrollId，首页数据source
        String scrollId = response.getScrollId();
        System.out.println("-----------首页------------");
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
        while (true) {
            //循环-创建一个SearchScrollRequest
            //指定ScrollId
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            //指定生存时间
            scrollRequest.scroll(TimeValue.timeValueMillis(1L));
            //执行scroll查询
            SearchResponse searchResponse = esClient.scroll(scrollRequest, RequestOptions.DEFAULT);
            //获取下一页数据
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            if (searchHits == null || searchHits.length <= 0) {
                System.out.println("-----------结束----------");
                break;
            }
            System.out.println("---------下一页数据--------");
            for (SearchHit hit : searchHits) {
                System.out.println(hit.getSourceAsMap());
            }
        }

        //取完数据之后，清楚内存的占用
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        //指定scrollId
        clearScrollRequest.addScrollId(scrollId);
        //删除ScrollId
        ClearScrollResponse clearScrollResponse = esClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        //输出删除scroll的结果
        System.out.println("删除scroll的结果:" + clearScrollResponse.isSucceeded());
    }


    @Test
    public void testDeleteByQuery() throws IOException {
        //创建request
        DeleteByQueryRequest request = new DeleteByQueryRequest(index);
        //指定检索条件
        request.setQuery(QueryBuilders.rangeQuery("fee").lt(4));
        //执行删除
        BulkByScrollResponse response = esClient.deleteByQuery(request, RequestOptions.DEFAULT);
        //输出结果
        System.out.println(response.toString());
    }

    @Test
    public void testBoolQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //省份为武汉或者是北京
        boolQuery.should(QueryBuilders.termQuery("province", "北京"));
        boolQuery.should(QueryBuilders.termQuery("province", "武汉"));
        //运营商不是联通
        boolQuery.mustNot(QueryBuilders.termQuery("operatorId", 2));
        //smsContent中包含中国和平安
        boolQuery.must(QueryBuilders.matchQuery("smsContent", "中国"));
        boolQuery.must(QueryBuilders.matchQuery("smsContent", "平安"));
        sourceBuilder.query(boolQuery);
        sourceBuilder.from(0);
        sourceBuilder.size(20);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    @Test
    public void testBoostingQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoostingQueryBuilder boostingQueryBuilder = QueryBuilders.boostingQuery(
                QueryBuilders.matchQuery("smsContent", "收货安装"),
                QueryBuilders.matchQuery("smsContent", "王五"))
                .negativeBoost(0.5f);
        sourceBuilder.query(boostingQueryBuilder);
        sourceBuilder.from(0);
        sourceBuilder.size(20);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    @Test
    public void testFilterQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.filter(QueryBuilders.termQuery("corpName", "盒马鲜生"));
        boolQuery.filter(QueryBuilders.rangeQuery("fee").lte(5));
        sourceBuilder.query(boolQuery);
        sourceBuilder.from(0);
        sourceBuilder.size(20);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    @Test
    public void testHighlightQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("smsContent", "盒马"));
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("smsContent", 10)
                .preTags("<em>")
                .postTags("</em>");
        sourceBuilder.highlighter(highlightBuilder);
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields.get("smsContent"));
        }

    }

    @Test
    public void testAggCardinality() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        CardinalityAggregationBuilder cardinalityAggregationBuilder = AggregationBuilders.cardinality("agg").field("province");
        sourceBuilder.aggregation(cardinalityAggregationBuilder);
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取结果
        Cardinality agg = response.getAggregations().get("agg");
        System.out.println(agg.getValue());
    }

    @Test
    public void testAggRange() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        RangeAggregationBuilder rangeAggregationBuilder = AggregationBuilders
                .range("agg")
                .field("fee")
                .addUnboundedTo(5)
                .addRange(5, 10)
                .addUnboundedFrom(10);
        sourceBuilder.aggregation(rangeAggregationBuilder);
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取结果
        Range agg = response.getAggregations().get("agg");
        List<? extends Range.Bucket> buckets = agg.getBuckets();
        for (Range.Bucket bucket : buckets) {
            System.out.println(bucket.getKey());
            System.out.println(bucket.getFrom() + "-" + bucket.getTo());
            System.out.println(bucket.getDocCount());
            System.out.println("------------------");
        }
    }

    @Test
    public void testAggExtendedStats() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest(index);

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        ExtendedStatsAggregationBuilder extendedStatsAggregationBuilder = AggregationBuilders
                .extendedStats("agg")
                .field("fee");
        sourceBuilder.aggregation(extendedStatsAggregationBuilder);
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取结果
        ExtendedStats agg = response.getAggregations().get("agg");
        System.out.println("最大值：" + agg.getMax());
        System.out.println("平均值：" + agg.getAvg());
    }

    @Test
    public void testGeoPolygonQuery() throws IOException {
        //创建Request对象
        SearchRequest request = new SearchRequest("map");

        //指定查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(39.99242,116.302269));
        points.add(new GeoPoint(39.915104,116.403968));
        points.add(new GeoPoint(39.921843,116.326346));
        GeoPolygonQueryBuilder geoPolygonQueryBuilder = QueryBuilders.geoPolygonQuery("location", points);
        sourceBuilder.query(geoPolygonQueryBuilder);
        request.source(sourceBuilder);

        //执行查询
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        //获取到_source中的数据
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

}
