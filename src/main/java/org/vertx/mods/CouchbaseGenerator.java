package org.vertx.mods;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.couchbase.client.ClusterManager;
import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.clustermanager.AuthType;
import com.couchbase.client.clustermanager.BucketType;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.ViewDesign;

/**
 * Class responsible of managing couchbase initialization, it performs
 * bucket creation/update besides views
 * 
 * @author JuanManuel
 *
 */
public abstract class CouchbaseGenerator extends BusModBase {

  private static final String MODE_PRODUCTION = "production";
  private static final String DEV_PREFIX = "dev_";
  
  // host and port
  protected String host;
  protected int port;
  
  //couchbase server username
	protected String username;
	//couchbase server password
	protected String password;
	
	// bucket config
	protected String bucketName;
  protected String bucketPassword;
	protected int bucketMemorySize;
  protected JsonArray views;
  
  // timeouts
  protected int operationTimeout;
  protected int viewTimeout;
  protected int observerTimeout;
  protected int viewConnsPerNode;
  
  // client and cluster manager
  protected ClusterManager manager;
  protected CouchbaseClient client;
  
  /**
   * Method that starts couchbase initialization process
   */
  protected void initializeCouchbase() {
    // grab configuration
    username = getOptionalStringConfig("username", null);
    password = getOptionalStringConfig("password", null);
    bucketMemorySize = getOptionalIntConfig("bucket_memory_size", 512);
    views = getOptionalArrayConfig("views", null);
    
    try {
      // execute generation
      generate();
    } catch (Exception e) {
      logger.error("Failed to generate to couchbase instance", e);
      throw new RuntimeException(e);
    } finally {
      if (manager != null) {
        manager.shutdown();
      }
    }
  }
  
  /**
   * Method to instance the couchbase client that will interact with the server
   */
  protected void createClient() {
    try {
      CouchbaseConnectionFactoryBuilder builder = new CouchbaseConnectionFactoryBuilder();
      builder.setOpTimeout(operationTimeout);
      builder.setViewTimeout(viewTimeout);
      builder.setObsTimeout(observerTimeout);
      builder.setViewConnsPerNode(viewConnsPerNode);
      // normally the worker size should be equal to the number of processors
      // org.apache.http.impl.nio.reactor.IOReactorConfig.AVAIL_PROCS
      builder.setViewWorkerSize(Runtime.getRuntime().availableProcessors());

      client = new CouchbaseClient(builder.buildCouchbaseConnection(bootstrapUris(), bucketName, bucketPassword));
    } catch (Exception e) {
      logger.error("Failed to connect to couchbase server", e);
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Method that orchestrate the couchbase server initialization
   * 
   * @throws Exception
   */
  private void generate() throws Exception {    
    container.logger().info("Connecting to " +  getURL() + " as " + username);

    manager = new ClusterManager(bootstrapUris(), username, password);

    // Get a list of all buckets in the cluster.
    List<String> buckets = manager.listBuckets();
    // Check if a bucket named "mybucket" exists in the cluster:
    if (!buckets.contains(bucketName)) {
      // Create a bucket with authentication.
      createBucket();
      // TODO improve this, give sometime to create the bucket
      Thread.sleep(5000);
    } else {
      // Update a bucket
      updateBucket();
    }

    // generate views whether are requested
    if (views != null) {
      generateViews();
    }
    // otherwise instance couchbase client
    else {
      createClient();
    }
  }
  
  /**
   * Method to create a bucket
   * @throws Exception
   */
  private void createBucket() throws Exception {
    manager.createNamedBucket(BucketType.COUCHBASE, bucketName, bucketMemorySize, 1, bucketPassword, true);
  }
  
  /**
   * Method to update a bucket
   * @throws Exception
   */
  private void updateBucket() throws Exception {
    manager.updateBucket(bucketName, bucketMemorySize, AuthType.SASL, 1, 11212, bucketPassword, true);
  }
  
  /**
   * Method to generate couchbase views
   * @throws IOException
   * @throws URISyntaxException
   */
  private void generateViews() throws IOException, URISyntaxException {
    final Map<String, DesignDocument> docs = new HashMap<String, DesignDocument>();

    Iterator<Object> it = views.iterator();
    while (it.hasNext()) {
      JsonObject view = (JsonObject)it.next();
      
      String name = view.getString("name");
      String designDoc = view.getString("designDoc");
      String function = view.getString("function");
          
      container.logger().info("Creating View " + name);
  
      // create views
      if (name != null && designDoc != null && function != null) {
        String mode = view.getString("mode", DEV_PREFIX);
        String reduce = view.getString("reduce", "");
        
        designDoc = mode.equals(MODE_PRODUCTION) ? designDoc : DEV_PREFIX + designDoc;
        
        ViewDesign viewDesign = new ViewDesign(name, function, reduce);
        
        // check if we already created a design document
        DesignDocument dDoc = docs.get(designDoc);
        if (dDoc == null) {
          dDoc = new DesignDocument(designDoc);
          docs.put(designDoc, dDoc);
        }
        
        container.logger().info("Creating View " + name + "on design document " + designDoc);
        
        dDoc.getViews().add(viewDesign);
      }
    }

    // instance client
    createClient();
    
    // create the design documents
    for (DesignDocument designDoc : docs.values()) {
        client.createDesignDoc(designDoc);
    }
  }
  
  private List<URI> bootstrapUris() throws URISyntaxException {
    return asList(new URI(getURL()));
  }
  
  private String getURL() {
    return "http://" + host + ":" + port + "/pools";
  }
}
