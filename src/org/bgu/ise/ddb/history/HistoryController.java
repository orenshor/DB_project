/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.operation.OrderBy;

import static com.mongodb.client.model.Filters.*;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController {

	MongoClient mongoClient;
	MongoDatabase mongoDatabase;
	MongoCollection<Document> mongoCollection;

	private void openMongoConnection() {
		try {
			this.mongoClient = new MongoClient("localhost", 27017);
			this.mongoDatabase = this.mongoClient.getDatabase("db3Project");
			this.mongoCollection = mongoDatabase.getCollection("History");
			System.out.println("Connection Established");
		} catch (Exception e) {
			this.mongoClient.close();
			e.printStackTrace();
		}
	}

	/**
	 * The function inserts to the system storage triple(s)(username, title,
	 * timestamp). The timestamp - in ms since 1970 Advice: better to insert the
	 * history into two structures( tables) in order to extract it fast one with the
	 * key - username, another with the key - title
	 * 
	 * @param username
	 * @param title
	 * @param response
	 */
	@RequestMapping(value = "insert_to_history", method = { RequestMethod.GET })
	public void insertToHistory(@RequestParam("username") String username, @RequestParam("title") String title,
			HttpServletResponse response) {
		openMongoConnection();
		System.out.println(username + " " + title);
		Document newHistoy = new Document("username", username).append("title", title).append("viewTimestamp",
				System.currentTimeMillis());
		mongoCollection.insertOne(newHistoy);
		try {
			this.mongoClient.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}

	/**
	 * The function retrieves users' history The function return array of pairs
	 * <title,viewtime> sorted by VIEWTIME in descending order
	 * 
	 * @param username
	 * @return
	 */
	@RequestMapping(value = "get_history_by_users", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public HistoryPair[] getHistoryByUser(@RequestParam("entity") String username) {
		List<HistoryPair> allViews = findViews(username, "title", "username");
		return allViews.toArray(new HistoryPair[allViews.size()]);
	}

	private List<HistoryPair> findViews(String reqValue, String reqField, String searchBy) {
		openMongoConnection();
		List<HistoryPair> allViews = new ArrayList<HistoryPair>();
		MongoCursor<Document> cursor = mongoCollection.find(eq(searchBy, reqValue)).sort(new BasicDBObject("viewTimestamp", OrderBy.DESC)).iterator();
		try {
			while (cursor.hasNext()) {
				Document next = cursor.next();
				String requireField = next.getString(reqField);
				Long viewTS = next.getLong("viewTimestamp");
				allViews.add(new HistoryPair(requireField, new Date(viewTS)));
			}
		} finally {
			cursor.close();
		}

		try {
			this.mongoClient.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return allViews;
	}

	/**
	 * The function retrieves items' history The function return array of pairs
	 * <username,viewtime> sorted by VIEWTIME in descending order
	 * 
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_history_by_items", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public HistoryPair[] getHistoryByItems(@RequestParam("entity") String title) {
		List<HistoryPair> allViews = findViews(title, "username", "title");
		return allViews.toArray(new HistoryPair[allViews.size()]);
	}

	/**
	 * The function retrieves all the users that have viewed the given item
	 * 
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_users_by_item", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public User[] getUsersByItem(@RequestParam("title") String title) {
		// :TODO your implementation
		User hp = new User("aa", "aa", "aa");
		System.out.println(hp);
		return new User[] { hp };
	}

	/**
	 * The function calculates the similarity score using Jaccard similarity
	 * function: sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|, where U(i)
	 * is the set of usernames which exist in the history of the item i.
	 * 
	 * @param title1
	 * @param title2
	 * @return
	 */
	@RequestMapping(value = "get_items_similarity", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	public double getItemsSimilarity(@RequestParam("title1") String title1, @RequestParam("title2") String title2) {
		// :TODO your implementation
		double ret = 0.0;
		return ret;
	}

}
