package org.streamreasoning.rsp4j.csparql;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import org.streamreasoning.rsp4j.yasper.examples.RDFStream;

public class RSP4WACStreamGenerator {
    private static final Long TIMEOUT = 1_000l;
    private static final long AVAILABILITY_CHANGE_FREQ 	= 4_000l;
    private static final long LOCATION_CHANGE_FREQ 		= 5_000l;

    public static final String PERSON_LOCATION_STREAM 		= "http://wac_test/person_location_stream/";
    public static final String DEVICE_AVAILABILITY_STREAM 	= "http://wac_test/device_availability_stream/";
    
    private static final String RSP_DEMO_IRI_BASE 		= "http://aimas.cs.pub.ro/consert/ontologies/2021/09/rsp-demo#";
    private static final String RDF_TYPE_IRI			= "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    private static final String ASSERTION_SUBJECT_IRI 	= "http://pervasive.semanticweb.org/ont/2017/07/consert/core#assertionSubject";
    private static final String ASSERTION_OBJECT_IRI 	= "http://pervasive.semanticweb.org/ont/2017/07/consert/core#assertionObject";
    
    private final String[] availabilityOptions = new String[]{"available"}; //, "notAvailable"};
    private final String[] devices = new String[]{"blindsLab308", "hueLampLab308"};
    
    private final Map<String, DataStream<Graph>> activeStreams;
    private final AtomicBoolean isStreaming;
    private final Random randomGenerator;

    public RSP4WACStreamGenerator() {
        this.activeStreams = new HashMap<String, DataStream<Graph>>();
        this.isStreaming = new AtomicBoolean(false);
        randomGenerator = new Random(2021);
    }

    public DataStream<Graph> getStream(String streamURI) {
        if (!activeStreams.containsKey(streamURI)) {
            RDFStream stream = new RDFStream(streamURI);
            activeStreams.put(streamURI, stream);
        }
        return activeStreams.get(streamURI);
    }

    public void startStreaming() {
        if (!this.isStreaming.get()) {
            this.isStreaming.set(true);
            Runnable task = () -> {
                long ts = 0;
                while (this.isStreaming.get()) {
                    long finalTs = ts;
                    
                    //if (finalTs % AVAILABILITY_CHANGE_FREQ == 0) 
                    //	generateDeviceAvailabilityDataForStream(getStream(DEVICE_AVAILABILITY_STREAM), finalTs);
                    
                    if (finalTs > 0 && finalTs % LOCATION_CHANGE_FREQ == 0)
                    	generatePersonLocationDataForStream(getStream(PERSON_LOCATION_STREAM), finalTs);
                    
                    ts += TIMEOUT;
                    try {
                        Thread.sleep(TIMEOUT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            };

            Thread thread = new Thread(task);
            thread.start();
        }
    }
    
    private void generatePersonLocationDataForStream(DataStream<Graph> stream, long ts) {
        RDF instance = RDFUtils.getInstance();
        Graph graph = instance.createGraph();
        BlankNode assertionNode = instance.createBlankNode();
        IRI is_a = instance.createIRI(RDF_TYPE_IRI);
        
        // add LocatedAt assertion instance
        graph.add(assertionNode, is_a, instance.createIRI(RSP_DEMO_IRI_BASE + "LocatedAt"));
        
        // add assertionSubject alex
        graph.add(assertionNode, instance.createIRI(ASSERTION_SUBJECT_IRI), instance.createIRI(RSP_DEMO_IRI_BASE + "alex"));
        
        // add assertionObject lab308
        graph.add(assertionNode, instance.createIRI(ASSERTION_OBJECT_IRI), instance.createIRI(RSP_DEMO_IRI_BASE + "lab308"));
        
        // add LocatedAt graph contents to stream at current timestamp
        stream.put(graph, ts);
    }
    
    private void generateDeviceAvailabilityDataForStream(DataStream<Graph> stream, long ts) {
    	RDF instance = RDFUtils.getInstance();
        Graph graph = instance.createGraph();
        
        for (int i = 0; i < devices.length; i++) {
	        BlankNode assertionNode = instance.createBlankNode();
	        IRI is_a = instance.createIRI(RDF_TYPE_IRI);
	        
	        // add HasAvailabilityStatus assertion instance
	        graph.add(assertionNode, is_a, instance.createIRI(RSP_DEMO_IRI_BASE + "HasAvailabilityStatus"));
	        
	        // add assertionSubject blindsLab308
	        graph.add(assertionNode, instance.createIRI(ASSERTION_SUBJECT_IRI), instance.createIRI(RSP_DEMO_IRI_BASE + devices[i]));
	        
	        // add assertionObject available / not available
	        graph.add(assertionNode, instance.createIRI(ASSERTION_OBJECT_IRI), instance.createIRI(RSP_DEMO_IRI_BASE + selectRandomAvailability()));
        }
        
        // add LocatedAt graph contents to stream at current timestamp
        stream.put(graph, ts);
    }
    
    private String selectRandomAvailability() {
        int randomIndex = randomGenerator.nextInt(availabilityOptions.length);
        return availabilityOptions[randomIndex];
    }

    public void stopStreaming() {
        this.isStreaming.set(false);
    }
}
