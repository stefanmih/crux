package com.crux.cli;

import com.crux.query.FilterParser;
import com.crux.query.QueryExpression;
import com.crux.store.DocumentStore;
import com.crux.store.Entity;
import com.google.gson.Gson;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Very small command line interface for interacting with the
 * {@link DocumentStore}. It understands a handful of commands and
 * uses {@link FilterParser} to interpret filter expressions.
 */
public class CommandLine {
    private final DocumentStore store = new DocumentStore();
    private final FilterParser parser = new FilterParser();
    private final Gson gson = new Gson();

    public void run() {
        Scanner sc = new Scanner(System.in);
        while (sc.hasNextLine()) {
            String line = sc.nextLine().trim();
            if (line.isEmpty()) continue;
            if ("exit".equalsIgnoreCase(line)) break;
            try {
                handle(line);
            } catch (Exception e) {
                System.out.println("error: " + e.getMessage());
            }
        }
    }

    private void handle(String line) {
        if (line.startsWith("add entity")) {
            addEntity(line.substring("add entity".length()).trim());
        } else if (line.startsWith("delete entity")) {
            String id = line.substring("delete entity".length()).trim();
            store.delete(id);
            System.out.println("deleted " + id);
        } else if (line.startsWith("update entities where")) {
            updateEntities(line);
        } else if (line.startsWith("get entities using filter")) {
            String expr = line.substring("get entities using filter".length()).trim();
            QueryExpression q = parser.parse(expr);
            List<Entity> res = store.query(q);
            System.out.println(gson.toJson(res.stream().map(Entity::getFields).collect(Collectors.toList())));
        } else if (line.startsWith("get field")) {
            getField(line);
        } else if (line.startsWith("get some")) {
            getSome(line);
        } else if (line.startsWith("show history")) {
            String id = line.substring("show history".length()).trim();
            System.out.println(gson.toJson(store.getHistory(id)));
        } else {
            System.out.println("unknown command");
        }
    }

    private void addEntity(String rest) {
        int jsonStart = rest.indexOf('{');
        int jsonEnd = rest.lastIndexOf('}');
        String json = rest.substring(jsonStart, jsonEnd + 1);
        Map<String,Object> map = gson.fromJson(json, Map.class);
        String after = rest.substring(jsonEnd + 1).trim();
        if (after.startsWith("vector")) {
            int lb = after.indexOf('[');
            int rb = after.indexOf(']', lb);
            String arr = after.substring(lb+1, rb).trim();
            List<Double> nums = new ArrayList<>();
            if (!arr.isEmpty()) {
                for (String n : arr.split("\\s+")) nums.add(Double.parseDouble(n));
            }
            map.put("vector", nums);
        }
        String id = (String) map.getOrDefault("id", UUID.randomUUID().toString());
        map.put("id", id);
        store.insert(new Entity(id, map));
        System.out.println("inserted " + id);
    }

    private void updateEntities(String line) {
        int whereIdx = line.indexOf("where") + 5;
        int setIdx = line.indexOf(" set ", whereIdx);
        String filterStr = line.substring(whereIdx, setIdx).trim();
        String json = line.substring(line.indexOf('{', setIdx), line.lastIndexOf('}') + 1);
        QueryExpression q = parser.parse(filterStr);
        Map<String,Object> upd = gson.fromJson(json, Map.class);
        List<Entity> matches = store.query(q);
        for (Entity e : matches) {
            store.updatePartial(e.getId(), upd);
        }
        System.out.println("updated " + matches.size());
    }

    private void getField(String line) {
        // get field FIELD from ID
        String rest = line.substring("get field".length()).trim();
        int fromIdx = rest.indexOf(" from ");
        String field = rest.substring(0, fromIdx).trim();
        String id = rest.substring(fromIdx + 6).trim();
        Entity e = store.get(id);
        if (e == null) { System.out.println("null"); return; }
        Object val = getFieldValue(e, field);
        System.out.println(gson.toJson(val));
    }

    private void getSome(String line) {
        int lb = line.indexOf('[');
        int rb = line.indexOf(']', lb);
        int n = 5;
        if (lb != -1 && rb != -1) {
            n = Integer.parseInt(line.substring(lb+1, rb).trim());
        }
        List<Map<String,Object>> result = new ArrayList<>();
        int i = 0;
        for (Entity e : store.findAll()) {
            if (i++ >= n) break;
            result.add(e.getFields());
        }
        System.out.println(gson.toJson(result));
    }

    private Object getFieldValue(Entity e, String path) {
        String[] parts = path.split("\\.");
        Object current = e.getFields();
        for (String p : parts) {
            if (current instanceof Map m) {
                current = m.get(p);
            } else if (current instanceof List l) {
                int idx = Integer.parseInt(p);
                current = l.get(idx);
            } else return null;
        }
        return current;
    }
}

