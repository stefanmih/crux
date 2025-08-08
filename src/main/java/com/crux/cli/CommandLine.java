package com.crux.cli;

import com.crux.query.FilterParser;
import com.crux.query.FilterParser.ValueExpression;
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
    private final Map<String, Set<String>> sets = new HashMap<>();
    private Map<String, ValueExpression> transformFunction = new HashMap<>();

    public CommandLine() {
        sets.put("all", new HashSet<>());
    }

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
            for (Set<String> s : sets.values()) s.remove(id);
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
        } else if (line.startsWith("generate")) {
            generate(line);
        } else if (line.startsWith("find similar")) {
            findSimilar(line);
        } else if (line.startsWith("create transform function")) {
            createTransformFunction(line);
        } else if (line.startsWith("apply transform function")) {
            applyTransformFunction(line);
        } else if (line.equalsIgnoreCase("help")) {
            printHelp();
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
        sets.get("all").add(id);
        System.out.println("inserted " + id);
    }

    private void updateEntities(String line) {
        String lower = line.toLowerCase(Locale.ROOT);
        int whereIdx = lower.indexOf("where");
        if (whereIdx == -1) throw new IllegalArgumentException("missing where clause");
        whereIdx += 5;
        int setIdx = lower.indexOf(" set", whereIdx);
        if (setIdx == -1) throw new IllegalArgumentException("missing set clause");
        String filterStr = line.substring(whereIdx, setIdx).trim();
        int jsonStart = line.indexOf('{', setIdx);
        int jsonEnd = line.lastIndexOf('}');
        if (jsonStart == -1 || jsonEnd == -1 || jsonEnd < jsonStart) {
            throw new IllegalArgumentException("missing update json");
        }
        String json = line.substring(jsonStart, jsonEnd + 1);
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

    private void generate(String line) {
        int n = Integer.parseInt(line.substring("generate".length()).trim());
        Random rnd = new Random();
        for (int i = 0; i < n; i++) {
            Map<String,Object> m = new HashMap<>();
            String id = UUID.randomUUID().toString();
            m.put("id", id);
            m.put("value", rnd.nextInt(1000));
            List<Double> vec = new ArrayList<>();
            for (int j = 0; j < 3; j++) vec.add(rnd.nextDouble());
            m.put("vector", vec);
            store.insert(new Entity(id, m));
            sets.get("all").add(id);
        }
        System.out.println("generated " + n);
    }

    private void findSimilar(String line) {
        String rest = line.substring("find similar".length()).trim();
        String[] parts = rest.split("\\s+");
        String id = parts[0];
        int top = parts.length > 1 ? Integer.parseInt(parts[1]) : 5;
        List<Entity> res = store.findSimilar(id, top);
        System.out.println(gson.toJson(res.stream().map(Entity::getFields).collect(Collectors.toList())));
    }

    private void createTransformFunction(String line) {
        int lb = line.indexOf('{');
        int rb = line.lastIndexOf('}');
        if (lb == -1 || rb == -1 || rb < lb) throw new IllegalArgumentException("missing body");
        String body = line.substring(lb+1, rb).trim();
        transformFunction = new HashMap<>();
        for (String part : body.split("[;\n]")) {
            part = part.trim();
            if (part.isEmpty()) continue;
            int arrow = part.indexOf("->");
            if (arrow == -1) continue;
            String exprStr = part.substring(0, arrow).trim();
            String field = part.substring(arrow+2).trim();
            ValueExpression ve = parser.parseValueExpression(exprStr);
            transformFunction.put(field, ve);
        }
        System.out.println("transform function created");
    }

    private void applyTransformFunction(String line) {
        String lower = line.toLowerCase(Locale.ROOT);
        int fromIdx = lower.indexOf("from set");
        if (fromIdx == -1) throw new IllegalArgumentException("missing from set");
        fromIdx += "from set".length();
        int toIdx = lower.indexOf("to", fromIdx);
        if (toIdx == -1) throw new IllegalArgumentException("missing to");
        String setName = line.substring(fromIdx, toIdx).trim();
        String newSet = line.substring(toIdx + 2).trim();
        Set<String> ids = sets.getOrDefault(setName, Collections.emptySet());
        Set<String> target = sets.computeIfAbsent(newSet, k -> new HashSet<>());
        int count = 0;
        for (String id : ids) {
            Entity e = store.get(id);
            if (e == null) continue;
            Map<String,Object> nf = new HashMap<>();
            for (var entry : transformFunction.entrySet()) {
                Object val = entry.getValue().eval(e);
                putFieldValue(nf, entry.getKey(), val);
            }
            String nid = UUID.randomUUID().toString();
            nf.put("id", nid);
            store.insert(new Entity(nid, nf));
            sets.get("all").add(nid);
            target.add(nid);
            count++;
        }
        System.out.println("transformed " + count);
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

    private void putFieldValue(Map<String,Object> map, String path, Object value) {
        String[] parts = path.split("\\.");
        Map<String,Object> current = map;
        for (int i = 0; i < parts.length-1; i++) {
            current = (Map<String,Object>) current.computeIfAbsent(parts[i], k -> new HashMap<>());
        }
        current.put(parts[parts.length-1], value);
    }

    private void printHelp() {
        System.out.println("Available commands:");
        System.out.println(" add entity {json} [vector [n1 n2 ...]]");
        System.out.println(" delete entity ID");
        System.out.println(" update entities where <filter> set {json}");
        System.out.println(" get entities using filter <filter>");
        System.out.println(" get field <path> from <id>");
        System.out.println(" get some [N]");
        System.out.println(" generate N");
        System.out.println(" find similar <id> [N]");
        System.out.println(" show history <id>");
        System.out.println(" create transform function { expr -> field }");
        System.out.println(" apply transform function from set <src> to <dest>");
        System.out.println(" help");
        System.out.println(" exit");
    }
}

