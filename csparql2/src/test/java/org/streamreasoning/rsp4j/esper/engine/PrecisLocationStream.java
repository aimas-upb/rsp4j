package org.streamreasoning.rsp4j.esper.engine;


import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.*;
import org.apache.jena.shared.uuid.JenaUUID;
import org.apache.jena.vocabulary.RDF;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import org.streamreasoning.rsp4j.io.DataStreamImpl;


/**
 * Created by Alexandru on 07/04/2023.
 */
@Log4j
public class PrecisLocationStream extends DataStreamImpl implements Runnable {

    private static final String PRECIS_IRI 		        = "http://aimas.cs.pub.ro/consert/ontologies/precis#";
    private static final String ANNOTATION_IRI 	        = "http://pervasive.semanticweb.org/ont/2017/07/consert/annotation#";
    private static final String TIMESTAMP_ANN           = ANNOTATION_IRI + "NumericTimestampAnnotation";
    private static final String ANN_HAS_VALUE           = ANNOTATION_IRI + "hasValue";

    private static final String ASSERTION_SUBJECT_IRI 	= "http://pervasive.semanticweb.org/ont/2017/07/consert/core#assertionSubject";
    private static final String ASSERTION_OBJECT_IRI 	= "http://pervasive.semanticweb.org/ont/2017/07/consert/core#assertionObject";

    private static final String LOCATED_AT_ASSERTION    = PRECIS_IRI + "LocatedAt";
    private static final String SUBJECT_ALEX            = PRECIS_IRI + "alex";
    private static final String OBJECT_LAB308           = PRECIS_IRI + "lab308";

    protected int publish_every_millis;
    private DataStream<Graph> s;

    private String type;

    public PrecisLocationStream(String name, String stream_uri, int grow_rate) {
        super(stream_uri);
        this.type = name;
        this.publish_every_millis = grow_rate * 1000;
    }

    public void setWritable(DataStream<Graph> e) {
        this.s = e;
    }

    private Model generatePersonLocationDataForStream(int generationTimestamp) {
        Model m = ModelFactory.createDefaultModel();
        
        // generate a Jena URN for the PersonLocation assertion instance
        Resource assertionInstance = m.createResource(stream_uri + "/" + JenaUUID.generate().asUUID().toString());

        m.add(m.createStatement(assertionInstance, RDF.type, 
            ResourceFactory.createResource(LOCATED_AT_ASSERTION)));
        m.add(m.createStatement(assertionInstance, 
            ResourceFactory.createProperty(ASSERTION_SUBJECT_IRI), 
            ResourceFactory.createResource(SUBJECT_ALEX)));
        m.add(m.createStatement(assertionInstance, 
            ResourceFactory.createProperty(ASSERTION_OBJECT_IRI), 
            ResourceFactory.createResource(OBJECT_LAB308)));

        // add the timestamp annotation
        Resource timestampAnnotation = m.createResource(JenaUUID.generate().asURN());
        m.add(m.createStatement(timestampAnnotation, RDF.type, TIMESTAMP_ANN));
        m.add(m.createStatement(timestampAnnotation, 
            ResourceFactory.createProperty(ANN_HAS_VALUE), 
            m.createTypedLiteral(generationTimestamp)));
        
        // add the annotation to the assertion
        m.add(m.createStatement(assertionInstance, 
            ResourceFactory.createProperty(ANNOTATION_IRI + "hasAnnotation"), 
            timestampAnnotation));
        
        return m;
    }

    public void run() {
        int ts = 0;
        
        while (ts <= 20000) {
            Model m = generatePersonLocationDataForStream(ts);
            
            System.out.println("At [" + ts + "] [" + System.currentTimeMillis() + "] Sending [" + m.getGraph() + "] on " + stream_uri);

            if (s != null)
                this.s.put(m.getGraph(), ts);
            try {
                // log.info("Sleep");
                Thread.sleep(this.publish_every_millis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ts += this.publish_every_millis;
        }
    }
}
