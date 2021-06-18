# dynamicStorm
Apache Storm - Dynamic Azure Event Hub Spout

# Introduction 
Storm Adapter provides Spouts which reconnects and renews the Azure EventHub Configuration at runtime. 
There can be any service / api (Discovery Service REST API example) which provides the EventHub connectionString with its expiry. 
The renewal of the Storm Spout happens when the connection expiry time, fetched from the Discovery (REST API as an example) expires.

#Technical Requirements
This is a Java based solution. Please refer pom.xml to know all dependent libraries and their versions.

Maven
	
    <dependency>
	  <groupId>com.chaoppo.storm.dynamic.adpater</groupId>	
      <artifactId>storm-dynamic-adapter</artifactId>
      <version>0.1-SNAPSHOT</version>
    </dependency>

#Use Cases
	Storm Adapter provides list of Spouts based on the Subscritption Keys. 
	These Spouts has the capability to fetch the data from Azure EventHub and emit it to downstream Bolts 

# Getting Started / Steps

#Mandatory Parameter to be used to initialize "StormAdapterFactory"
	
	Following variables must be set by the Business Solution application .
	1.	SUBSCRIPTION_KEY
	2.	EVENTHUB_READ_URL

#1. Initialize the Storm Adapter

    StormAdapterFactory stormAdapterFactory = new StormAdapterFactory(SUBSCRIPTION_KEY, EVENTHUB_READ_URL)
    .withBulkMsgCount(BULK_MSG_COUNT); //default is 20


#2. Get the list of Azure EventHubs

    List<StormDynaSpout> eventHubInfoSet = stormAdapterFactory.getEventHubSpout();

#3. Set the Spout into Busines Solutions Topology

    TopologyBuilder topologyBuilder = new TopologyBuilder();
    AtomicInteger count = new AtomicInteger(0);
    eventHubInfoSet.forEach(e -> {
        // parallelism_hint should be equal to the number of Event Hub partitions
        topologyBuilder.setSpout(e.getSpoutName(), e.getSpout(), e.getPartitionCount());
        topologyBuilder.setBolt("SimpleBolt-" + count.incrementAndGet(), simpleBolt).shuffleGrouping(e.getSpoutName());
    });
    

#4. Once the topology is started the above Spout will connect to EventHub and start emitting msgs to downstream Bolts listening to it

    SimpleBolt - execute() method implementation to fetch the msgs and its properties

    @Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
        ...
        List<EventData> msgs = (List<EventData>) tuple.getValueByField(Constants.BULK_EVENT_HUB_SPOUT_EMIT_FIELD);
        //List<EventData> is emitted by the Spout
        //Constants.BULK_EVENT_HUB_SPOUT_EMIT_FIELD String value is "Msg"
        ...
        for (EventData msg : msgs) {
            String data = new String(msg.getBytes());
            // "data" emitted from the Spout. 
            //If Business Solution expects amqp msg then use msg.getObject();
			
            Map<String, Object> metaData = (Map<String, Object>) msg.getProperties();
            //Application Properties
            Map<String, Object> systemProps = msg.getSystemProperties();
            //System Properties
        }

#Storm Adapter E2E Testing [This module is not yet uploaded]
This modules does the Integration testing of the Storm Adapter. It used hadoop libraries to start local cluster and 
listens to Azure Event Hub in staging.
The test Bolt asserts the data emitted from the Azure Event Hub.

#Storm Usage [This module is not yet uploaded]
This modules contains below Use Cases with Example for Business Solutions to understand and implement the Storm Solution.

    1. Creating Spouts to receive data from EventHub, renew the token when connection is expired and reconnect with the 
       new EventHub configs
    2. Simple Bolt which listes to the Messages emitted by the Spout
    3. CommandServiceSenderBolt to send the Message to Device
    4. AzureHdfsBlobStorageBolt which downloads the file from Azure Blob using the url from the msg

#Other

 





