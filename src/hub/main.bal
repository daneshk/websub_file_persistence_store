import ballerina/io;
import ballerina/http;
import ballerina/runtime;
import ballerina/websub;

// The topic against which the publisher will publish updates and the subscribers
// need to subscribe to, to receive notifications when an order is placed.
const string ORDER_TOPIC = "http://localhost:9090/ordermgt/ordertopic";

// Define a `FilePersistenceStore` as a `websub:HubPersistenceStore`.
websub:HubPersistenceStore hubPersistenceStore = new FilePersistenceStore("logs");
// Set the defined persistence store as the `hubPersistenceStore` in the `hubConfig` record.
websub:HubConfiguration hubConfig = {
    hubPersistenceStore: hubPersistenceStore,
    remotePublish : {
        enabled : true
    }
};

public function main() {
    io:println("Starting up the Ballerina Hub Service");

    var result = websub:startHub(new http:Listener(9191), hubConfig);
    websub:WebSubHub webSubHub = result is websub:HubStartedUpError ?
                                               result.startedUpHub : result;
    var result1 = webSubHub.registerTopic(ORDER_TOPIC);
    while(true) {}
}
