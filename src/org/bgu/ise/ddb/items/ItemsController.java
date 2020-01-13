/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {

	private Connection oracleConnection;
	MongoClient mongoClient;
	MongoDatabase mongoDatabase;
	MongoCollection<Document> mongoCollection;

	public ItemsController() {
	}

	private void openOracleConnection() {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			this.oracleConnection = DriverManager.getConnection("jdbc:oracle:thin:@132.72.65.216:1521/ORACLE",
					"ilayfri", "ilayfRi140");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void openMongoConnection() {
		try {
			this.mongoClient = new MongoClient("localhost", 27017);
			this.mongoDatabase = this.mongoClient.getDatabase("db3Project");
			this.mongoCollection = mongoDatabase.getCollection("Mediaitems");
			System.out.println("Connection Established");
		} catch (Exception e) {
			this.mongoClient.close();
			e.printStackTrace();
		}
	}

	/**
	 * The function copy all the items(title and production year) from the Oracle
	 * table MediaItems to the System storage. The Oracle table and data should be
	 * used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method = { RequestMethod.GET })
	public void fillMediaItems(HttpServletResponse response) {
		System.out.println("was here");
		// :TODO your implementation
		openMongoConnection();
		openOracleConnection();
		PreparedStatement ps = null;
		try {
			ps = this.oracleConnection.prepareStatement("SELECT TITLE, PROD_YEAR FROM MEDIAITEMS");
			ResultSet r = ps.executeQuery();
			while (r.next()) {
				if (mongoCollection.find(eq("title", r.getNString("TITLE"))).first() != null) {
					Document newItem = new Document("title", r.getNString("TITLE")).append("prod_year",
							r.getInt("PROD_YEAR"));
					mongoCollection.insertOne(newItem);
				}
			}
			r.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		try {
			this.mongoClient.close();
			this.oracleConnection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}

	/**
	 * The function copy all the items from the remote file, the remote file have
	 * the same structure as the films file from the previous assignment. You can
	 * assume that the address protocol is http
	 * 
	 * @throws IOException
	 */
	@RequestMapping(value = "fill_media_items_from_url", method = { RequestMethod.GET })
	public void fillMediaItemsFromUrl(@RequestParam("url") String urladdress, HttpServletResponse response)
			throws IOException {
		System.out.println(urladdress);
		openMongoConnection();
		URL url = new URL(urladdress);
		BufferedReader br = null;
		String line = "";

		try {

			br = new BufferedReader(new InputStreamReader(url.openStream()));
			while ((line = br.readLine()) != null) {
				String[] mediaItem = line.split(",");
				if (mongoCollection.find(eq("title", mediaItem[0])).first() != null) {
					try {
						int prod_year = Integer.parseInt(mediaItem[1]);
						Document newItem = new Document("title", mediaItem[0]).append("prod_year", prod_year);
						mongoCollection.insertOne(newItem);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		try {
			this.mongoClient.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}

	/**
	 * The function retrieves from the system storage N items, order is not
	 * important( any N items)
	 * 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public MediaItems[] getTopNItems(@RequestParam("topn") int topN) {
		openMongoConnection();
		MongoCursor<Document> cursor = mongoCollection.find().limit(topN).iterator();
		List <MediaItems> allMediaItems = new ArrayList<MediaItems>();
		try {
		    while (cursor.hasNext()) {
		    	Document next = cursor.next();
		    	String title = next.getString("title");
		    	int prod_year = next.getInteger("prod_year");
		    	allMediaItems.add(new MediaItems(title, prod_year));
		    }
		} finally {
		    cursor.close();
		}
		try {
			this.mongoClient.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return allMediaItems.toArray(new MediaItems[allMediaItems.size()]);
	}

}
