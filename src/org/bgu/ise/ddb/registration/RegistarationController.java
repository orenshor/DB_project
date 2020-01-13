package org.bgu.ise.ddb.registration;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import static com.mongodb.client.model.Filters.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController {
	MongoClient mongoClient;
	MongoDatabase database;
	MongoCollection<Document> collection;

	public RegistarationController() {
	}

	private void openConnection() {
		try {
			this.mongoClient = new MongoClient("localhost", 27017);
			System.out.println("Connection Established");
			this.database = this.mongoClient.getDatabase("db3Project");
			this.collection = database.getCollection("users");
		} catch (Exception e) {
			this.mongoClient.close();
			e.printStackTrace();
		}
	}

	/**
	 * The function checks if the username exist, in case of positive answer
	 * HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT, else
	 * insert the user to the system and set to HttpStatus in HttpServletResponse
	 * HttpStatus.OK
	 * 
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method = { RequestMethod.POST })
	public void registerNewUser(@RequestParam("username") String username, @RequestParam("password") String password,
			@RequestParam("firstName") String firstName, @RequestParam("lastName") String lastName,
			HttpServletResponse response) {
		HttpStatus status;
		openConnection();
		System.out.println(username + " " + password + " " + lastName + " " + firstName);
		Document existUser = collection.find(eq("username", username)).first();
		if (existUser == null) {
			Document newUser = new Document("username", username).append("password", password)
					.append("firstName", firstName).append("lastName", lastName).append("registrationTimestamp", System.currentTimeMillis());
			collection.insertOne(newUser);
			System.out.println("User existed");
			status = HttpStatus.OK;
		} else {
			status = HttpStatus.CONFLICT;
		}
		response.setStatus(status.value());
		this.mongoClient.close();
	}

	/**
	 * The function returns true if the received username exist in the system
	 * otherwise false
	 * 
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method = { RequestMethod.GET })
	public boolean isExistUser(@RequestParam("username") String username) throws IOException {
		System.out.println("isExist: " + username);
		boolean result = false;
		openConnection();
		Document existUser = collection.find(eq("username", username)).first();
		if (existUser == null) {
			result = false;
		} else {
			result = true;
		}
		this.mongoClient.close();
		return result;
	}

	/**
	 * The function returns true if the received username and password match a
	 * system storage entry, otherwise false
	 * 
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method = { RequestMethod.POST })
	public boolean validateUser(@RequestParam("username") String username, @RequestParam("password") String password)
			throws IOException {
		System.out.println("validate: " + username + " " + password);
		boolean result = false;
		openConnection();
		Document existUser = collection.find(and(eq("password", password), eq("username", username))).first();
		if (existUser == null) {
			result = false;
		} else {
			result = true;
		}
		this.mongoClient.close();
//		getNumberOfRegistredUsers(3);
		return result;

	}

	/**
	 * The function retrieves number of the registered users in the past n days
	 * 
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method = { RequestMethod.GET })
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException {
		System.out.println(days + "");
		int result = 0;
		openConnection();
		long timeRequaired = System.currentTimeMillis() - (days * 24 * 60 * 60 * 1000);
		result = (int) collection.count(gte("registrationTimestamp", timeRequaired));
		System.out.println("count: " + result);
		this.mongoClient.close();
		return result;

	}

	/**
	 * The function retrieves all the users
	 * 
	 * @return
	 */
	@RequestMapping(value = "get_all_users", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public User[] getAllUsers() {
		openConnection();
		MongoCursor<Document> cursor = collection.find().iterator();
		List <User> allUsers = new ArrayList<User>();
		try {
		    while (cursor.hasNext()) {
		    	Document next = cursor.next();
		    	String username = next.getString("username");
		    	String password = next.getString("password");
		    	String firstName = next.getString("firstName");
		    	String lastName = next.getString("lastName");
		    	allUsers.add(new User(username, password, firstName, lastName));
		    }
		} finally {
		    cursor.close();
		}
//		User u = new User("alexk", "alex", "alex");
//		System.out.println(u);
		this.mongoClient.close();
		return allUsers.toArray(new User[allUsers.size()]);
	}

}
