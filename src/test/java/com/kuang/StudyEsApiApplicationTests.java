package com.kuang;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeFilter;
import com.kuang.pojo.User;
import net.minidev.json.JSONObject;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.ml.EvaluateDataFrameRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

@SpringBootTest
class StudyEsApiApplicationTests {

	//面向对象操作

	@Autowired
	private RestHighLevelClient restHighLevelClient;

	//测试索引的创建	Request
	@Test
	void testCreateIndex() throws IOException {
		//1、创建索引请求
		CreateIndexRequest requeset = new CreateIndexRequest("kuang_index");
		//2、执行创建请求	IndicesClient
		CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(requeset, RequestOptions.DEFAULT);
		System.out.println(createIndexResponse);

	}
	/**
	 * 查询索引
	 * @throws IOException
	 */
	@Test
	void testExistIndex() throws IOException {
		GetIndexRequest request = new GetIndexRequest("kuang_index");
		boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
		System.out.println(exists);
	}

	/**
	 * 删除索引
	 * @throws IOException
	 */
	@Test
	void testDeleteIndex() throws IOException {
		DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("kuang_index");
		AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
		System.out.println(delete);
	}

	/**
	 * 创建文档
	 * @throws IOException
	 */
	@Test
	void testCreateDocument() throws IOException {
		//创建请求
		IndexRequest request = new IndexRequest("kuang_index");
		//创建对象
//		User user = new User("张飞",1);
		User user = new User();
		//规则 Put kuang_index/_doc/1
		request.id("1");
		request.timeout(TimeValue.timeValueSeconds(1));
		request.timeout("1s");
		//将我们的数据请求放入json
		IndexRequest source = request.source(JSON.toJSONString(user), XContentType.JSON);
		//客户端发送请求，获取相应的结果
		IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);
		System.out.println(indexResponse.toString());
		System.out.println(indexResponse.status());
	}

	/**
	 * 文档是否存在
	 * @throws IOException
	 */
	@Test
	void testExistDocument() throws IOException {
		//testapi 索引中     是否存在 1 的文档
		GetRequest getRequest = new GetRequest("kuang_index", "1");
		boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
		System.out.println(exists);
	}

	/**
	 * 获取文档信息
	 * @throws IOException
	 */
	@Test
	void testGetDocument() throws IOException {
		GetRequest getRequest = new GetRequest("kuang_index", "1");
		GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
		System.out.println(getResponse.getSourceAsString());//获取文档内容
		System.out.println(getResponse.getSource());
	}

	/**
	 * 更新文档信息
	 * @throws IOException
	 */
	@Test
	void testUpdatDocument() throws IOException {
		UpdateRequest updateRequest = new UpdateRequest("kuang_index", "1");
//		User user = new User("张飞",18);
		User user = new User();
		updateRequest.doc(JSON.toJSONString(user),XContentType.JSON);
		UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
		System.out.println(update.status());
	}

	/**
	 * 删除文档信息
	 * @throws IOException
	 */
	@Test
	void testDeleteDocument() throws IOException {
		DeleteRequest request = new DeleteRequest("kuang_index", "1");
		request.timeout("1s");

		DeleteResponse delete = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
		System.out.println(delete.status());
	}

	/**
	 * Tess的，真正的项目一般都会批量插入数据
	 * @throws IOException
	 */
	@Test
	void testBulkDocument() throws IOException {
		BulkRequest bulkRequest = new BulkRequest();
		bulkRequest.timeout("10s");
		ArrayList<User> userList = new ArrayList<>();
//		userList.add(new User("123",1));
		userList.add(new User());
		userList.add(new User());
		userList.add(new User());
		userList.add(new User());
		//批处理请求
		for (int i = 0; i < userList.size(); i++) {
			//批量更新和批量删除，在这修改对应的请求就可以了
			bulkRequest.add(
					new IndexRequest("kuang_index")
					.id(""+(i+1))//不用的话，生成随机id
					.source(JSON.toJSONString(userList.get(i), (SerializeFilter) XContentType.JSON))
			);
		}
		BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
		System.out.println(bulkResponse.hasFailures());//是否失败 false代表成功
	}

	/**
	 * 搜索文档
	 */
	@Test
	void testSearchDocument() throws IOException {
		SearchRequest searchRequest = new SearchRequest("kuang_index");
		//匹配字段
		MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("username", "李白");
		//构建查询器
		searchRequest.source(new SearchSourceBuilder().query(matchQueryBuilder));
		SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
		System.out.println(searchResponse.getHits());
		System.out.println(searchResponse.getHits().getTotalHits());
	}
}
