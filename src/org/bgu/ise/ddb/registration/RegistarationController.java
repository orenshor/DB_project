/**
 * 
 */
package org.bgu.ise.ddb.registration;


import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.io.IOException;
import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import static com.mongodb.client.model.Filters.*;


/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController{
	MongoDatabase database;
	MongoCollection<Document> collection; 
	
	public RegistarationController() {
		MongoClient mongo = new MongoClient("127.0.0.1", 27017);
		System.out.println("Connection Established");
		this.database = mongo.getDatabase("db3Project");
		this.collection = database.getCollection("users");
	}
	/**
	 * The function checks if the username exist,
	 * in case of positive answer HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT,
	 * else insert the user to the system  and set to HttpStatus in HttpServletResponse HttpStatus.OK
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method={RequestMethod.POST})
	public void registerNewUser(@RequestParam("username") String username,
			@RequestParam("password")    String password,
			@RequestParam("firstName")   String firstName,
			@RequestParam("lastName")  String lastName,
			HttpServletResponse response){
		System.out.println(username+" "+password+" "+lastName+" "+firstName);
		Document existUser = collection.find(eq("username", username)).first();
		if(existUser == null) {
			System.out.println("oren");
			Document newUser = new Document("username", username)
	                .append("password", password)
	                .append("firstName", firstName)
	                .append("lastName", lastName);
			collection.insertOne(newUser);

		}
		//int size = existUser.size();
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
		
	}
	
	/**
	 * The function returns true if the received username exist in the system otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method={RequestMethod.GET})
	public boolean isExistUser(@RequestParam("username") String username) throws IOException{
		System.out.println(username);
		boolean result = false;
		//:TODO your implementation
		
		return result;
		
	}
	
	/**
	 * The function returns true if the received username and password match a system storage entry, otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method={RequestMethod.POST})
	public boolean validateUser(@RequestParam("username") String username,
			@RequestParam("password")    String password) throws IOException{
		System.out.println(username+" "+password);
		boolean result = false;
		//:TODO your implementation
		
		
		return result;
		
	}
	
	/**
	 * The function retrieves number of the registered users in the past n days
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method={RequestMethod.GET})
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException{
		System.out.println(days+"");
		int result = 0;
		//:TODO your implementation
		
		return result;
		
	}
	
	/**
	 * The function retrieves all the users
	 * @return
	 */
	@RequestMapping(value = "get_all_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public  User[] getAllUsers(){
		//:TODO your implementation
		User u = new User("alex", "alex", "alex");
		System.out.println(u);
		return new User[]{u};
	}

}
