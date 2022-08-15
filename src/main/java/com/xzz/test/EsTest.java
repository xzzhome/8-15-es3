package com.xzz.test;

import com.xzz.utils.ESClientUtil;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class EsTest {

    @Test
    public void testAdd() {
        //获取客户端对象
        TransportClient client = ESClientUtil.getClient();

        //创建索引
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("shopping", "user", "1");
        Map<String,Object> data = new HashMap<>();
        data.put("id",1);
        data.put("username","zs");
        data.put("age",11);
        //获取结果
        IndexResponse indexResponse = indexRequestBuilder.setSource(data).get();
        System.out.println(indexResponse);

        client.close();
    }

    @Test
    public void testGet() {
        //获取客户端对象
        TransportClient client = ESClientUtil.getClient();

        GetRequestBuilder requestBuilder = client.prepareGet("shopping", "user", "1");
        //获取结果
        Map<String, Object> source = requestBuilder.get().getSource();
        System.out.println(source);

        client.close();
    }

    @Test
    public void testUpdate() {
        //获取客户端对象
        TransportClient client = ESClientUtil.getClient();

        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate("shopping", "user", "1");
        Map<String,Object> data = new HashMap<>();
        data.put("id",1);
        data.put("username","李四");
        data.put("age",11);
        //获取结果
        UpdateRequestBuilder requestBuilder = updateRequestBuilder.setDoc(data);
        UpdateResponse response = requestBuilder.get();
        System.out.println(response);

        client.close();
    }

    @Test
    public void testDelete(){
        //获取客户端对象
        TransportClient client = ESClientUtil.getClient();

        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete("shopping", "user", "1");
        DeleteResponse deleteResponse = deleteRequestBuilder.get();

        System.out.println(deleteResponse);
        client.close();
    }

    @Test
    public void testBuilkAdd(){
        //获取客户端对象
        TransportClient client = ESClientUtil.getClient();

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        Map<String,Object> data1 = new HashMap<>();
        data1.put("id",11);
        data1.put("username","zs");
        data1.put("age",11);

        bulkRequestBuilder.add(client.prepareIndex("shopping", "user", "11").setSource(data1));

        Map<String,Object> data2 = new HashMap<>();
        data2.put("id",22);
        data2.put("username","zs");
        data2.put("age",11);

        bulkRequestBuilder.add(client.prepareIndex("shopping", "user", "11").setSource(data2));

        BulkResponse bulkItemResponses = bulkRequestBuilder.get();
        Iterator<BulkItemResponse> iterator = bulkItemResponses.iterator();
        while(iterator.hasNext()){
            BulkItemResponse next = iterator.next();
            System.out.println(next.getResponse());
        }
        client.close();
    }

    @Test
    public void testSearch(){
        //获取客户端对象
        TransportClient client = ESClientUtil.getClient();

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("shopping");

        //这是基本的数据处理
        searchRequestBuilder.setTypes("user");
        searchRequestBuilder.setFrom(0);
        searchRequestBuilder.setSize(10);
        searchRequestBuilder.addSort("age", SortOrder.ASC);

        //获取组合bool
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        //bool中的must，以及包含的match
        List<QueryBuilder> must = boolQueryBuilder.must();
        must.add(QueryBuilders.matchQuery("username" , "zs"));

        //过滤条件，bool中的filter
        List<QueryBuilder> filter = boolQueryBuilder.filter();
        //filter中的range和term
        filter.add(QueryBuilders.rangeQuery("age").lte(20).gte(10));
        filter.add(QueryBuilders.termQuery("id",11));

        searchRequestBuilder.setQuery(boolQueryBuilder);//将组合bool添加进入查询条件之中

        SearchResponse searchResponse = searchRequestBuilder.get();//获取查询内容

        SearchHits hits = searchResponse.getHits();

        System.out.println("条数："+hits.getTotalHits());
        for (SearchHit hit : hits.getHits()) {
            System.out.println(hit.getSourceAsMap());

        }

        client.close();
    }
}
