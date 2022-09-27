package org.streamreasoning.rsp4j.io.utils.parsing;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.log4j.Logger;
import org.streamreasoning.rsp4j.io.utils.RDFBase;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class JenaRDFParsingStrategy implements ParsingStrategy<Graph> {

    private static final Logger log = Logger.getLogger(JenaRDFParsingStrategy.class);
    private final RDFBase base;

    public JenaRDFParsingStrategy(RDFBase base) {
        this.base = base;
    }

    @Override
    public Graph parse(String parseString) {
        log.debug("Received for parsing: " + parseString);
        Model dataModel = ModelFactory.createDefaultModel();
        try {
            InputStream targetStream = new ByteArrayInputStream(parseString.getBytes());
            dataModel.read(targetStream, null, base.name());
            JenaRDF jena = new JenaRDF();

            Graph g1 = jena.asGraph(dataModel);
            return g1;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
}
