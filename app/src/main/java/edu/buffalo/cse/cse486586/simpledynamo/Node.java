package edu.buffalo.cse.cse486586.simpledynamo;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by sumedhas on 4/14/17.
 */

class Node
{
    private ConcurrentHashMap<String, String> data;
    private String next, prev;
    private String node_hash;
    private String origin;



    /* Constructor */
    public Node()
    {
        this.next = null;
        this.prev = null;
        this.data = null;
        this.node_hash = null;
        this.origin = null;
    }
    /* Constructor */
    public Node(ConcurrentHashMap<String, String> data, String next, String prev, String node_hash, String origin) {
        this.data = data;
        this.next = next;
        this.prev = prev;
        this.node_hash = node_hash;
        this.origin = origin;
    }

    public ConcurrentHashMap<String, String> getData() {
        return data;
    }

    public void setData(ConcurrentHashMap<String, String> data) {
        this.data = data;
    }

    public String getNext() {
        return next;
    }

    public void setNext(String next) {
        this.next = next;
    }

    public String getPrev() {
        return prev;
    }

    public void setPrev(String prev) {
        this.prev = prev;
    }

    public String getNode_hash() {
        return node_hash;
    }

    public void setNode_hash(String node_hash) {
        this.node_hash = node_hash;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }
}

