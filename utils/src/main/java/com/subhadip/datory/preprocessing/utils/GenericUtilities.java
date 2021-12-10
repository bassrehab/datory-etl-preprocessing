package com.subhadip.datory.preprocessing.utils;

import scala.Tuple2;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

public class GenericUtilities {

    public static ArrayList<String> getEnvClasspath(){
        ClassLoader cl = ClassLoader.getSystemClassLoader();


        URL[] urls = ((URLClassLoader)cl).getURLs();
        ArrayList<String> classURLs = new ArrayList<String>();
        for(URL url: urls){
            classURLs.add(url.getFile());
        }

        return classURLs;
    }

    @SuppressWarnings("unchecked")
    private static <K, V> scala.collection.immutable.Map<K, V> toScalaImmutableMap(java.util.Map<K, V> javaMap) {
        final java.util.List<scala.Tuple2<K, V>> list = new java.util.ArrayList<>(javaMap.size());
        for (final java.util.Map.Entry<K, V> entry : javaMap.entrySet()) {
            list.add(scala.Tuple2.apply(entry.getKey(), entry.getValue()));
        }
        final scala.collection.Seq<Tuple2<K, V>> seq = scala.collection.JavaConverters.asScalaBufferConverter(list).asScala().toSeq();
        return (scala.collection.immutable.Map<K, V>) scala.collection.immutable.Map$.MODULE$.apply(seq);
    }
}
