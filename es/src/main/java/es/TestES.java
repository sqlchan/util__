package es;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.inject.internal.SourceProvider;
import org.elasticsearch.common.inject.internal.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.swing.text.Highlighter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TestES {

    private volatile static TransportClient client ;
    public static TransportClient getClient() throws UnknownHostException {
        if(null == client){
            synchronized (TransportClient.class){
                //指定集群名称
                //Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
                client = new PreBuiltTransportClient(Settings.EMPTY)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            }
        }
        return client;
    }

    public static void main(String[] args)  {

        GetResponse response = client.prepareGet("hik", "mac", "1").get();
        System.out.println(response.getSourceAsString());

//==============索引管理>>>>>>>>>>>>>>>>>
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        IndicesExistsResponse exResponse = indicesAdminClient.prepareExists("indexName").get();
        System.out.println(exResponse.isExists());

        TypesExistsResponse existsResponse = indicesAdminClient.prepareTypesExists("indexName")
                .setTypes("type1","type2").get();
        System.out.println(existsResponse.isExists());

        CreateIndexResponse cResponse = indicesAdminClient.prepareCreate("indexName").get();
        System.out.println(cResponse.isAcknowledged());

        CreateIndexResponse cResponse1 = indicesAdminClient.prepareCreate("indexName")
                .setSettings( Settings.builder().put("index.num_or_shards",3) ).get();

        UpdateSettingsResponse upResponse = indicesAdminClient.prepareUpdateSettings("indexName")
                .setSettings( Settings.builder().put("index.num_or_shards",3) ).get();

        GetSettingsResponse getResponse = indicesAdminClient.prepareGetSettings("indexName","typeName").get();
        for(ObjectObjectCursor<String, Settings> cursor : getResponse.getIndexToSettings()){
            String index = cursor.key;
            Settings settings = cursor.value;
            Integer shards = settings.getAsInt("index.num_or_shards",null);
        }

        DeleteIndexResponse deleteIndexResponse = indicesAdminClient.prepareDelete("indexName").get();
        System.out.println(deleteIndexResponse.isAcknowledged());

        indicesAdminClient.prepareRefresh("indexName").get();
        CloseIndexResponse closeIndexResponse = indicesAdminClient.prepareClose("indexNama").get();

//>>>>>>>>>>>>>索引管理=============================

//===========文档管理>>>>>>>>>>>>>>
        //新建文档
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("macAddress","111111111"+(int)(Math.random()*100));
        json.put("postDate",new Date());
        json.put("devNo","dev00"+(int)Math.random());
        IndexResponse response1 = client.prepareIndex("hik", "mac").setSource(json).get();
        System.out.println(response1.status());
        // 获取文档
        GetResponse response2 = client.prepareGet("hik","mac","1").get();
        String content = response2.getSourceAsString();
        //删除文档
        DeleteResponse response3 = client.prepareDelete("hik","mac","1").get();
        System.out.println(response3.status());
        //批量获取
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("hik","mac","1").add("hik","mac","1").get();
        for(MultiGetItemResponse itemResponses : multiGetItemResponses){
            GetResponse response4 = itemResponses.getResponse();
            System.out.println(response4.getSourceAsString());
        }

        //搜索
        QueryBuilder matchQuery =QueryBuilders.matchQuery("title","java")
                .operator(Operator.AND);
        HighlightBuilder highlighter = new HighlightBuilder().field("title");
        SearchResponse response4 = client.prepareSearch("books")
                .setQuery(matchQuery)
                .highlighter(highlighter)
                .setSize(100)
                .get();
        SearchHits hits = response4.getHits();
        System.out.println("搜索到： "+hits.getTotalHits()+" 条数据.");
        for(SearchHit hit : hits){
            System.out.println("source: "+hit.getSourceAsString());
            System.out.println("source as map : "+ hit.getSource());
            System.out.println("price: "+hit.getSource().get("price"));
        }
        //全文查询
        QueryBuilder matchallqurey = QueryBuilders.matchAllQuery();
        QueryBuilder queryPhraseQuery = QueryBuilders.matchPhraseQuery("foo","hello world");
        QueryBuilder queryPhrasePrefixQuery = QueryBuilders.matchPhrasePrefixQuery("foo","hello world");
        QueryBuilder queryMatchQuery = QueryBuilders.multiMatchQuery("ky","user","msg");
        QueryBuilder commonTermsQuery = QueryBuilders.commonTermsQuery("name","ky");
        QueryBuilder queryStringQuery = QueryBuilders.queryStringQuery("+ky -es");
        QueryBuilder qb = QueryBuilders.simpleQueryStringQuery("+ky -es");




//        GetResponse response1 = client.prepareGet("hik", "mac", "1").setOperationThreaded(false).get();



    }
}
