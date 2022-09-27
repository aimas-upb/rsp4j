package org.streamreasoning.rsp4j.reasoning.csprite;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;
import org.streamreasoning.rsp4j.api.operators.r2r.utils.R2RPipe;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class UpwardExtensionTest {

    @Test
    public void testSimpleHierarchy() {
        Map<String, List<String>> schema = new HashMap<>();
        schema.put("O2", Arrays.asList("O1", "O4"));
        schema.put("O3", Arrays.asList("O2", "O5"));
        schema.put("O5", Arrays.asList("O6"));
        RelationToRelationOperator<Graph, Graph> r2r = new R2RUpwardExtension(schema);


        Graph graph = RDFUtils.createGraph();
        IRI p = RDFUtils.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        graph.add(RDFUtils.createTriple(RDFUtils.createIRI("S1"), p, RDFUtils.createIRI("O2")));
        graph.add(RDFUtils.createTriple(RDFUtils.createIRI("S1"), RDFUtils.createIRI("P1"), RDFUtils.createIRI("O2")));
        Stream<Graph> result = r2r.eval(Stream.of(graph));


        Graph expected = RDFUtils.createGraph();
        expected.add(RDFUtils.createTriple(RDFUtils.createIRI("S1"), p, RDFUtils.createIRI("O2")));
        expected.add(RDFUtils.createTriple(RDFUtils.createIRI("S1"), p, RDFUtils.createIRI("O3")));
        expected.add(RDFUtils.createTriple(RDFUtils.createIRI("S1"), RDFUtils.createIRI("P1"), RDFUtils.createIRI("O2")));

        assertEquals(expected.stream().collect(Collectors.toSet()), result.findFirst().get().stream().collect(Collectors.toSet()));

    }

    @Test
    public void testSimpleHierarchyQueryTest() {
        Map<String, List<String>> schema = new HashMap<>();
        schema.put("O2", Arrays.asList("O1", "O4"));
        schema.put("O3", Arrays.asList("O2", "O5"));
        schema.put("O5", Arrays.asList("O6"));
        RelationToRelationOperator<Graph, Graph> r2r = new R2RUpwardExtension(schema);
        VarOrTerm s1 = new VarImpl("s");
        VarOrTerm p1 = new TermImpl("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        VarOrTerm o1 = new TermImpl("O3");
        TP tp = new TP(s1, p1, o1);

        Graph graph = RDFUtils.createGraph();
        IRI p = RDFUtils.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        graph.add(RDFUtils.createTriple(RDFUtils.createIRI("S1"), p, RDFUtils.createIRI("O2")));
        graph.add(RDFUtils.createTriple(RDFUtils.createIRI("S1"), RDFUtils.createIRI("P1"), RDFUtils.createIRI("O2")));

        R2RPipe<Graph, Binding> r2rPipe = new R2RPipe<>(r2r, tp);
        Stream<Binding> result = r2rPipe.eval(Stream.of(graph));
        Binding b1 = new BindingImpl();
        b1.add(new VarImpl("s"), RDFUtils.createIRI("S1"));

        assertEquals(b1, result.findFirst().get());

    }

}
