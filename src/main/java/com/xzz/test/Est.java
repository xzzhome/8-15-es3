package com.xzz.test;

import com.xzz.utils.ESClientUtil;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Est {

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
    public void test1(){
        TransportClient client = ESClientUtil.getClient();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("shopping");
        searchRequestBuilder.setFrom(0);
        searchRequestBuilder.setSize(10);
        searchRequestBuilder.addSort("age", SortOrder.ASC);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        boolQueryBuilder.must(QueryBuilders.matchQuery("username" , "zs"));

        boolQueryBuilder.filter(QueryBuilders.termQuery("id",11))
                        .filter(QueryBuilders.rangeQuery("age").lte(20).gte(10));

        searchRequestBuilder.setQuery(boolQueryBuilder);

        client.close();
    }
}
