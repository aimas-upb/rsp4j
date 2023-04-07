package org.streamreasoning.rsp4j.esper.engine;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.Graph;
import org.streamreasoning.rsp4j.api.engine.config.EngineConfiguration;
import org.streamreasoning.rsp4j.api.querying.ContinuousQuery;
import org.streamreasoning.rsp4j.api.sds.SDSConfiguration;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import org.streamreasoning.rsp4j.csparql2.engine.CSPARQLEngine;
import org.streamreasoning.rsp4j.csparql2.engine.JenaContinuousQueryExecution;
import org.streamreasoning.rsp4j.csparql2.sysout.ResponseFormatterFactory;

public class TestSharedCtxIdentification {
    static final String STREAM_IRI  = "http://aimas.cs.pub.ro/consert/PersonLocatedStream";
    
    static String PRECIS_PREFIX     = "http://aimas.cs.pub.ro/consert/ontologies/precis#";
    static String QUERY_NAME        = "test-shared-ctx-ident";
    static String QUERY_SUFFIX      = ".rspql";
    
    static CSPARQLEngine sr;

    public static void main(String[] args) throws InterruptedException, IOException, ConfigurationException {

        
        String path = TestSharedCtxIdentification.class.getResource("/csparql-ctx-ident.properties").getPath();
        SDSConfiguration config = new SDSConfiguration(path);
        EngineConfiguration ec = EngineConfiguration.loadConfig("/csparql-ctx-ident.properties");

        sr = new CSPARQLEngine(0, ec);

        PrecisLocationStream writer = new PrecisLocationStream("<ploc>", STREAM_IRI, 5);
        DataStream<Graph> register = sr.register(writer);
        writer.setWritable(register);

        JenaContinuousQueryExecution cqe = (JenaContinuousQueryExecution)sr.register(getQuery(QUERY_NAME, QUERY_SUFFIX), config);
        ContinuousQuery query = cqe.query();

        System.out.println(query.toString());
        System.out.println("<<------>>");
        
        // set the Stream output format for the continuous query as Turtle, knowing that the query is a CONSTRUCT query
        if (query.isConstructType()) {
            cqe.addQueryFormatter(ResponseFormatterFactory.getConstructResponseSysOutFormatter("Turtle", false));
        } else if (query.isSelectType()) {
            cqe.addQueryFormatter(ResponseFormatterFactory.getSelectResponseSysOutFormatter("JSON-LD", false)); //or "CSV" or "JSON" or "JSON-LD"
        }

        // Create a consumer for the output stream of the query - in this case we print the output to the console
        DataStream outputStream = cqe.outstream();
        // if (outputStream != null)
        //     outputStream.addConsumer((o, l) -> { System.out.println(o); System.out.println("<<------------>>");  });

        // Start the stream writer in a separate thread - for quick testing purposes
        (new Thread(writer)).start();
    }

    public static String getQuery(String queryName, String suffix) throws IOException {
        URL resource = TestSharedCtxIdentification.class.getResource("/" + queryName + suffix);
        System.out.println(resource.getPath());
        File file = new File(resource.getPath());
        return FileUtils.readFileToString(file);
    }
}
