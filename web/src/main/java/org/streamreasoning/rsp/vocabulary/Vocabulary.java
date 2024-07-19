package org.streamreasoning.rsp.vocabulary;

import java.util.ServiceLoader;


import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;

public class Vocabulary {

    public static RDF is;

    static {
         ServiceLoader<RDF> loader = ServiceLoader.load(RDF.class);
         is = loader.iterator().next();
    }

    public static IRI resource(String uri, String local) {
    	return is.createIRI(uri + local);
    }

    public static Triple triple(BlankNodeOrIRI s, IRI p, RDFTerm o) {
         return is.createTriple(s, p, o);
    }

}
