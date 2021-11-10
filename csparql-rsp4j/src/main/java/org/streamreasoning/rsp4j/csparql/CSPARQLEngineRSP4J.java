package org.streamreasoning.rsp4j.csparql;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.stream.Collectors;

import org.apache.commons.rdf.api.Graph;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.engine.features.StreamRegistrationFeature;
import org.streamreasoning.rsp4j.api.querying.ContinuousQuery;
import org.streamreasoning.rsp4j.api.querying.ContinuousQueryExecution;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.Binding;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.BindingImpl;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.VarImpl;

import eu.larkc.csparql.cep.api.RdfStream;
import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.core.ResultFormatter;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import eu.larkc.csparql.core.streams.formats.CSparqlQuery;

public class CSPARQLEngineRSP4J  implements StreamRegistrationFeature<DataStream<Graph>, DataStream<Graph>> {

    private final CsparqlEngineImpl engine;
    private boolean activateInference;
    private DataStream<Binding> outputSelect;
    private DataStream<Graph> outputConstruct;
    private Map<CsparqlQueryResultProxy,CSPARQLAbstractQuery> proxyToQuery;
    public CSPARQLEngineRSP4J() {
        this.engine = new CsparqlEngineImpl();
        engine.initialize(true);
        activateInference = false;
        proxyToQuery = new HashMap<>();
    }
    
    public void putStaticNamedModel(String iri, String modelReference) {
    	engine.putStaticNamedModel(iri, modelReference);
    }
    
    public void removeStaticNamedModel(String iri) {
		engine.removeStaticNamedModel(iri);
	}
    
    public void addDefaultModel(String modelReference) {
    	engine.addDefaultModel(modelReference);
    }
    
	public RDFTable evaluateGeneralQueryOverDatasource(String queryBody) {
		return engine.evaluateGeneralQueryOverDatasource(queryBody);
	}
    
	public RDFTable evaluateGeneralQuery(String queryBody) {
		return engine.evaluateGeneralQuery(queryBody);
	}
	
    @Override
    public DataStream<Graph> register(DataStream<Graph> s) {
        RdfStream wrappedStream = new RDFStreamWrapper(s);
        engine.registerStream(wrappedStream);
        return s;
    }

    public ContinuousQuery<Graph, Graph, Binding, Binding> parseCSPARQLSelect(String query1) {
        try {
            CsparqlQueryResultProxy proxy = engine.registerQuery(query1, activateInference);
            CSparqlQuery csparqlQuery = engine.getAllQueries().stream().filter(q -> q.getId().equals(proxy.getId())).findAny().get();
            CSPARQLAbstractQuery<Binding> abstractQuery = new CSPARQLAbstractQuery<>(proxy, csparqlQuery.getSparqlQuery().getQueryCommand());
            //register listener
            proxyToQuery.put(proxy,abstractQuery);

            return abstractQuery;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
    public ContinuousQuery<Graph, Graph, Binding, Graph> parseCSPARQLConstruct(String query1) {
        try {
            CsparqlQueryResultProxy proxy = engine.registerQuery(query1, activateInference);
            CSparqlQuery csparqlQuery = engine.getAllQueries().stream().filter(q -> q.getId().equals(proxy.getId())).findAny().get();
            CSPARQLAbstractQuery<Graph> abstractQuery = new CSPARQLAbstractQuery<>(proxy, csparqlQuery.getSparqlQuery().getQueryCommand());
            //register listener
            proxyToQuery.put(proxy,abstractQuery);

            return abstractQuery;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ContinuousQueryExecution<Graph, Graph, Binding, Binding> parseSelect(ContinuousQuery<Graph, Graph, Binding, Binding> cq) {
        CsparqlQueryResultProxy proxy = inverseMap(proxyToQuery).get(cq);
        proxy.addObserver(new WrappedSelectListener());

        return new CSPARQLQueryExecution<>(cq,outputSelect);
    }
    public ContinuousQueryExecution<Graph, Graph, Binding, Graph> parseConstruct(ContinuousQuery<Graph, Graph, Binding, Graph> cq) {
        CsparqlQueryResultProxy proxy = inverseMap(proxyToQuery).get(cq);
        proxy.addObserver(new WrappedConstructListener());

        return new CSPARQLQueryExecution(cq,outputConstruct);
    }


    public void setActivateInference(boolean activateInference){
        this.activateInference = activateInference;
    }
    public DataStream<Binding> setSelectOutput(DataStream<Binding> outputStream){
        this.outputSelect = outputStream;
        return outputSelect;
    }
    public DataStream<Graph> setConstructOutput(DataStream<Graph> outputStream){
        this.outputConstruct = outputStream;
        return outputConstruct;
    }



    private class WrappedSelectListener extends ResultFormatter{

        public void update(Observable o, Object arg) {
            CsparqlQueryResultProxy proxy = (CsparqlQueryResultProxy) o;
            CSPARQLAbstractQuery abstractQuery = proxyToQuery.get(proxy);
            List<String> varNames = abstractQuery.getVariables();
            RDFTable q = (RDFTable)arg;
            long time = System.currentTimeMillis();
            Iterator var4 = q.iterator();
            Binding b = new BindingImpl();

            while(var4.hasNext()) {
                RDFTuple t = (RDFTuple)var4.next();
                for(int i = 0 ; i <varNames.size(); i++){
                    b.add(new VarImpl(varNames.get(i)), RDFUtils.createIRI(t.get(i)));
                }
                outputSelect.put(b,time);
            }


        }
    }
    private class WrappedConstructListener extends ResultFormatter{

        public void update(Observable o, Object arg) {

            RDFTable q = (RDFTable)arg;
            long time = System.currentTimeMillis();
            Iterator var4 = q.iterator();
            Graph commonsGraph = RDFUtils.createGraph();

            while(var4.hasNext()) {
                RDFTuple t = (RDFTuple)var4.next();
                commonsGraph.add(RDFUtils.createTriple(RDFUtils.createIRI(t.get(0)),
                        RDFUtils.createIRI(t.get(1)),
                        RDFUtils.createIRI(t.get(2))));

            }
            outputConstruct.put(commonsGraph,time);


        }
    }
    public static <K, V> Map<V, K> inverseMap(Map<K, V> sourceMap) {
        return sourceMap.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey,
                        (a, b) -> a) //if sourceMap has duplicate values, keep only first
        );
    }
}
