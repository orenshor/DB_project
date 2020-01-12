/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.IOException;
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
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;



/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {
	
	private Connection connection;


	public ItemsController() {
		 try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            this.connection = DriverManager.getConnection("jdbc:oracle:thin:@132.72.65.216:1521/ORACLE", "ilayfri", "ilayfRi140");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
	}
	
	/**
	 * The function copy all the items(title and production year) from the Oracle table MediaItems to the System storage.
	 * The Oracle table and data should be used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method={RequestMethod.GET})
	public void fillMediaItems(HttpServletResponse response){
		System.out.println("was here");
		//:TODO your implementation
//		List<String, Integer> allTheItems = new ArrayList<String, Integer>();
//        PreparedStatement ps = null;
//        try {
//            ps = this.connection.prepareStatement("SELECT * FROM MEDIAITEMS");
//            ResultSet r = ps.executeQuery();
//            while ( r.next() ) {
//                Long mid = r.getLong("MID");
//                allTheItems.add(mid);
//
//            }
//            r.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }finally {
//            try {
//                ps.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//        return allTheItems;
    
		
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	
	

	/**
	 * The function copy all the items from the remote file,
	 * the remote file have the same structure as the films file from the previous assignment.
	 * You can assume that the address protocol is http
	 * @throws IOException 
	 */
	@RequestMapping(value = "fill_media_items_from_url", method={RequestMethod.GET})
	public void fillMediaItemsFromUrl(@RequestParam("url")    String urladdress,
			HttpServletResponse response) throws IOException{
		System.out.println(urladdress);
		
		//:TODO your implementation
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	
	
	/**
	 * The function retrieves from the system storage N items,
	 * order is not important( any N items) 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public  MediaItems[] getTopNItems(@RequestParam("topn")    int topN){
		//:TODO your implementation
		MediaItems m = new MediaItems("Game of Thrones", 2011);
		System.out.println(m);
		return new MediaItems[]{m};
	}
		

}
