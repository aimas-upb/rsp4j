package org.streamreasoning.rsp4j.cqels;

import java.io.File;


import java.io.StringWriter;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.log4j.PropertyConfigurator;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;
import org.streamreasoning.rsp4j.api.querying.ContinuousQuery;
import org.streamreasoning.rsp4j.api.querying.ContinuousQueryExecution;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import main.CQELSEngineRSP4J;
import org.streamreasoning.rsp4j.cqels.RSP4WACStreamGenerator;
import org.streamreasoning.rsp4j.io.DataStreamImpl;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.Binding;

//import eu.larkc.cqels.common.RDFTable;

public class TestRSP4WAC {
	
	private static class WACTestConsumer implements Consumer<Graph> {

		@Override
		public void notify(Graph g, long ts) {
			String res = "";
			for (Triple t : g.iterate()) {
				res += t.getSubject().ntriplesString() + " " + t.getPredicate().ntriplesString() + " " + t.getObject().ntriplesString(); 
				res += "\n";
			}
			
			System.out.println(res + "@" + ts);
		}
		
	}
	
	public static void main(String[] args) throws InterruptedException {
		System.out.println("Welcome");
		System.out.println("Welcome");
		System.out.println("Welcome");
		String log4jConfigFile = System.getProperty("user.dir")
                + File.separator + "log4j.properties";
		PropertyConfigurator.configure(log4jConfigFile);
		
		CQELSEngineRSP4J cqelsEngine = new CQELSEngineRSP4J();
        
		String rspDemoStaticContentIri = "urn:x-arq:DefaultGraph";
		//String rspDemoStaticContentIri = "rsp-demo.ttl";
		Model model = RDFDataMgr.loadModel("ontology/rsp-demo.ttl") ;
		
		StringWriter sw = new StringWriter();
		model.write(sw, "TURTLE");
		
		//cqelsEngine.putStaticNamedModel(rspDemoStaticContentIri, sw.toString());
		//cqelsEngine.addDefaultModel(sw.toString());
		
		// GENERATE THE SIMULATED STREAM
		RSP4WACStreamGenerator generator = new RSP4WACStreamGenerator();
        DataStream<Graph> personLocationStream = generator.getStream(RSP4WACStreamGenerator.PERSON_LOCATION_STREAM);
        DataStream<Graph> deviceAvailabilityStream = generator.getStream(RSP4WACStreamGenerator.DEVICE_AVAILABILITY_STREAM);
        
        DataStream<Graph> wacOutputStream = new DataStreamImpl<>("http://out/stream");       
        String wacStreamingQuery =
				// "REGISTER QUERY GetWAC AS \n"
        		 "PREFIX rsp-demo: <http://aimas.cs.pub.ro/consert/ontologies/2021/09/rsp-demo#> " 
				+ "PREFIX consert-core: <http://pervasive.semanticweb.org/ont/2017/07/consert/core#> "
				+ "PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#> "
				+ "PREFIX acl: 	<http://www.w3.org/ns/auth/acl#> "
				+ "CONSTRUCT {" 
				+ "		" + "[]" + " rdf:type " + " acl:Authorization ; "
				+ "			" + " acl:accessTo " 	+ "rsp-demo:blinds308 ; "
				+ "			" + " acl:mode " 		+ "acl:Read ; "
				+ "			" + " acl:agent " 		+ "?person . "
				+ "} "
				+ " WHERE { "
				+ " STREAM " + " <" + RSP4WACStreamGenerator.PERSON_LOCATION_STREAM + "> " + "[NOW] {"
				+ "		?personLocAssertion rdf:type rsp-demo:LocatedAt ."
				+ "		?personLocAssertion consert-core:assertionSubject ?person ."
				+ "		?personLocAssertion consert-core:assertionObject ?loc ."
			    + "}"
			    + " STREAM " + " <" + RSP4WACStreamGenerator.DEVICE_AVAILABILITY_STREAM + "> " + "[NOW] {" 
				+ "		?availabilityAssertion rdf:type rsp-demo:HasAvailabilityStatus ."
				+ "		?availabilityAssertion consert-core:assertionSubject ?device ."
				+ "		?availabilityAssertion consert-core:assertionObject rsp-demo:available ."
				+ "}"
				+ " GRAPH " + " <" + rspDemoStaticContentIri + "> " + " {"
				+ "		?device rdf:type rsp-demo:Device ."
				+ "		?loc rdf:type rsp-demo:Room ."
				+ "		?locAssertion rdf:type rsp-demo:LocatedAt ."
				+ "		?locAssertion consert-core:assertionSubject ?device ."
				+ "		?locAssertion consert-core:assertionObject ?loc ."
				+ "}"

				//+ "		?org rdf:type rsp-demo:Organization ."

//				+ "		?ownsAssertion rdf:type rsp-demo:OwnedBy ."
//				+ "		?ownsAssertion consert-core:assertionSubject ?device ."
//				+ "		?ownsAssertion consert-core:assertionObject ?org ."
				
				// + "		?person rdf:type rsp-demo:Person ." 
				
//				+ "		?worksAtAssertion rdf:type rsp-demo:WorksAt ."
//				+ "		?worksAtAssertion consert-core:assertionSubject ?person ."
//				+ "		?worksAtAssertion consert-core:assertionObject ?org ."

				+ "}";
        
        cqelsEngine.register(personLocationStream);
        cqelsEngine.register(deviceAvailabilityStream);
        cqelsEngine.setConstructOutput(wacOutputStream);
        
//        cqelsEngine.addDefaultModel(sw.toString());
        ContinuousQuery<Graph, Binding, Binding, Graph> cq = cqelsEngine.parseCQELSConstruct(wacStreamingQuery);

//problematic:        
        ContinuousQueryExecution<Graph, Binding, Binding, Graph> cqe = cqelsEngine.parseConstruct(cq);

//        //wacOutputStream.addConsumer(new WACTestConsumer());
        wacOutputStream.addConsumer((el,ts)->System.out.println(el + " @ " + ts));

        generator.startStreaming();
        Thread.sleep(20_000);
        generator.stopStreaming();

	}
}
