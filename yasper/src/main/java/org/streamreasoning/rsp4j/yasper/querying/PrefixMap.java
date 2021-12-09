package org.streamreasoning.rsp4j.yasper.querying;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class PrefixMap {

    private final Map<String, String> prefixes;

    public PrefixMap(){
        this.prefixes = new HashMap<>();
    }
    public void addPrefix(String prefixName, String iri){
        if(prefixName.endsWith(":")){
            prefixName = prefixName.substring(0,prefixName.length()-1);
        }
        this.prefixes.put(prefixName, iri);
    }
    public Optional<String> getPrefix(String prefixName){
        return Optional.ofNullable(this.prefixes.get(prefixName));
    }
    public String expandIfPrefixed(String iri){
        if (iri.contains(":")) {
          String[] split = iri.split(":");
          if(prefixes.containsKey(split[0])){
              return String.format("%s%s",prefixes.get(split[0]),split[1]);
          }
        }
        return iri;
    }
}
