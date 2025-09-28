package com.crux.cli;

import com.crux.query.FilterParser;
import com.crux.query.FilterParser.ValueExpression;
import com.crux.query.QueryExpression;
import com.crux.store.DocumentStore;
import com.crux.store.Entity;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Very small command line interface for interacting with the
 * {@link DocumentStore}. It understands a handful of commands and
 * uses {@link FilterParser} to interpret filter expressions.
 */
public class CommandLine {
    private static final Logger LOGGER = Logger.getLogger(CommandLine.class.getName());
    private static final List<HelpEntry> HELP_ENTRIES;

    final DocumentStore store = new DocumentStore();
    final FilterParser parser = new FilterParser();
    final Gson gson = new Gson();
    final Map<String, Set<String>> sets = new HashMap<>();
    final CommandParser commandParser = new CommandParser();
    Map<String, ValueExpression> transformFunction = new HashMap<>();

    static {
        List<HelpEntry> entries = new ArrayList<>();
        entries.add(new HelpEntry(
                "add entity {json} [vector [n1 n2 ...]]",
                "Insert a JSON entity. The optional vector accepts whitespace separated doubles.",
                "add", "insert"));
        entries.add(new HelpEntry(
                "delete entity <id>",
                "Delete an entity by id. The entity is removed from all sets.",
                "delete", "remove"));
        entries.add(new HelpEntry(
                "update entities where <filter> set {json}",
                "Update all entities matching the filter with the provided JSON body.",
                "update"));
        entries.add(new HelpEntry(
                "get entities using filter <filter>",
                "Return all entities matching the filter expression as JSON.",
                "get entities", "query"));
        entries.add(new HelpEntry(
                "get field <path> from <id>",
                "Read a nested field (dot or index notation) from the entity.",
                "get field"));
        entries.add(new HelpEntry(
                "get some [N]",
                "Print up to N entities from the store (default 5).",
                "get some", "list"));
        entries.add(new HelpEntry(
                "generate <N>",
                "Generate N synthetic entities with random values.",
                "generate"));
        entries.add(new HelpEntry(
                "find similar <id> [N]",
                "Return the top N (default 5) entities similar to the given id based on vector distance.",
                "find", "similar"));
        entries.add(new HelpEntry(
                "show history <id>",
                "Display change history for an entity.",
                "show", "history"));
        entries.add(new HelpEntry(
                "create transform function { <expr> -> <field>; ... }",
                "Define a reusable transformation from existing entities into a new set of entities.",
                "create", "transform"));
        entries.add(new HelpEntry(
                "apply transform function from set <src> to <dest>",
                "Execute the active transform function against every entity in <src> and store the result in <dest>.",
                "apply", "transform"));
        entries.add(new HelpEntry(
                "persist snapshot",
                "Persist the current document store snapshot to disk.",
                "persist", "snapshot"));
        entries.add(new HelpEntry(
                "help [command]",
                "Show this summary or detailed help for a specific command.",
                "help", "-h", "--help", "?"));
        entries.add(new HelpEntry(
                "exit",
                "Exit the interactive shell.",
                "exit", "quit"));
        HELP_ENTRIES = Collections.unmodifiableList(entries);
    }

    private record HelpEntry(String usage, String description, Set<String> keywords) {
        HelpEntry(String usage, String description, String... keywords) {
            this(usage, description, normaliseKeywords(keywords, usage));
        }

        private static Set<String> normaliseKeywords(String[] keywords, String usage) {
            Set<String> keys = new LinkedHashSet<>();
            String firstWord = usage.split("\\s+")[0].toLowerCase(Locale.ROOT);
            keys.add(firstWord);
            if (keywords != null) {
                for (String keyword : keywords) {
                    if (keyword != null && !keyword.isBlank()) {
                        keys.add(keyword.toLowerCase(Locale.ROOT));
                    }
                }
            }
            return keys;
        }

        boolean matches(String topic) {
            String lowered = topic.toLowerCase(Locale.ROOT);
            if (keywords.contains(lowered)) return true;
            return usage.toLowerCase(Locale.ROOT).startsWith(lowered);
        }
    }

    public CommandLine() {
        sets.put("all", new HashSet<>(store.getAllIds()));
    }

    public void run() {
        Scanner sc = new Scanner(System.in);
        System.out.println("Welcome to the Crux CLI! Type 'help' to see available commands.");
        final String prompt = "> ";
        System.out.print(prompt);
        while (sc.hasNextLine()) {
            String line = sc.nextLine().trim();
            if (line.isEmpty()) {
                System.out.print(prompt);
                continue;
            }
            if ("exit".equalsIgnoreCase(line)) break;
            try {
                handle(line);
            } catch (CliException e) {
                if (LOGGER.isLoggable(Level.FINE) || e.getCause() != null) {
                    LOGGER.log(Level.FINE, "User error while executing command: " + line, e);
                }
                System.out.println("error: " + e.getMessage());
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to execute command: " + line, e);
                System.out.println("error: " + e.getMessage());
            }
            System.out.print(prompt);
        }
    }

    void handle(String line) {
        CommandParser.Command cmd = commandParser.parse(line);
        cmd.execute(this);
    }

    void addEntity(String rest) {
        try {
            int jsonStart = rest.indexOf('{');
            int jsonEnd = rest.lastIndexOf('}');
            if (jsonStart == -1 || jsonEnd == -1 || jsonEnd < jsonStart) {
                throw new CliException("expected JSON body: add entity {\"id\":\"123\"}");
            }
            String json = rest.substring(jsonStart, jsonEnd + 1);
            Map<String,Object> map = parseJsonDocument(json, "entity body", true);
            String after = rest.substring(jsonEnd + 1).trim();
            if (after.startsWith("vector")) {
                int lb = after.indexOf('[');
                int rb = after.indexOf(']', lb);
                if (lb == -1 || rb == -1 || rb < lb) {
                    throw new CliException("vector section must be in square brackets, e.g. vector [0.1 0.2]");
                }
                String arr = after.substring(lb+1, rb).trim();
                List<Double> nums = new ArrayList<>();
                if (!arr.isEmpty()) {
                    for (String n : arr.split("\\s+")) {
                        try {
                            nums.add(Double.parseDouble(n));
                        } catch (NumberFormatException ex) {
                            throw new CliException("vector values must be numeric: '" + n + "'", ex);
                        }
                    }
                }
                map.put("vector", nums);
            }
            String id = (String) map.getOrDefault("id", UUID.randomUUID().toString());
            map.put("id", id);
            store.insert(new Entity(id, map));
            sets.get("all").add(id);
            System.out.println("inserted " + id);
        } catch (CliException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw new CliException(e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to add entity: " + rest, e);
            throw new CliException("unable to insert entity: " + e.getMessage(), e);
        }
    }

    void updateEntities(String line) {
        try {
            String lower = line.toLowerCase(Locale.ROOT);
            int whereIdx = lower.indexOf("where");
            if (whereIdx == -1) throw new CliException("missing 'where' clause. Usage: update entities where <filter> set {json}");
            whereIdx += 5;
            int setIdx = lower.indexOf(" set", whereIdx);
            if (setIdx == -1) throw new CliException("missing 'set' clause. Usage: update entities where <filter> set {json}");
            String filterStr = line.substring(whereIdx, setIdx).trim();
            int jsonStart = line.indexOf('{', setIdx);
            int jsonEnd = line.lastIndexOf('}');
            if (jsonStart == -1 || jsonEnd == -1 || jsonEnd < jsonStart) {
                throw new CliException("expected JSON body after set clause");
            }
            String json = line.substring(jsonStart, jsonEnd + 1);
            QueryExpression q = parser.parse(filterStr);
            Map<String,Object> upd = parseJsonDocument(json, "update body", false);
            List<Entity> matches = store.query(q);
            for (Entity e : matches) {
                store.updatePartial(e.getId(), upd);
            }
            System.out.println("updated " + matches.size());
        } catch (CliException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw new CliException(e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to update entities with line: " + line, e);
            throw new CliException("unable to update entities: " + e.getMessage(), e);
        }
    }

    void printField(String field, String id) {
        if (field == null || field.isBlank() || id == null || id.isBlank()) {
            throw new CliException("usage: get field <path> from <id>");
        }
        try {
            Entity e = store.get(id);
            if (e == null) { System.out.println("null"); return; }
            Object val = getFieldValue(e, field);
            System.out.println(gson.toJson(val));
        } catch (CliException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw new CliException(e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get field '" + field + "' from id '" + id + "'", e);
            throw new CliException("unable to read field: " + e.getMessage(), e);
        }
    }

    void printSome(int n) {
        if (n < 0) {
            throw new CliException("number of entities must be non-negative");
        }
        try {
            List<Map<String,Object>> result = new ArrayList<>();
            int i = 0;
            for (Entity e : store.findAll()) {
                if (i++ >= n) break;
                result.add(e.getFields());
            }
            System.out.println(gson.toJson(result));
        } catch (CliException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to list entities", e);
            throw new CliException("unable to fetch entities: " + e.getMessage(), e);
        }
    }

    void generate(String line) {
        try {
            String argument = line.substring("generate".length()).trim();
            if (argument.isEmpty()) {
                throw new CliException("usage: generate <count>");
            }
            int n;
            try {
                n = Integer.parseInt(argument);
            } catch (NumberFormatException ex) {
                throw new CliException("count must be an integer", ex);
            }
            if (n <= 0) {
                throw new CliException("count must be greater than zero");
            }
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
        } catch (CliException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw new CliException(e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to generate with line: " + line, e);
            throw new CliException("unable to generate entities: " + e.getMessage(), e);
        }
    }

    void findSimilar(String line) {
        try {
            String rest = line.substring("find similar".length()).trim();
            if (rest.isEmpty()) {
                throw new CliException("usage: find similar <id> [topN]");
            }
            String[] parts = rest.split("\\s+");
            String id = parts[0];
            int top = 5;
            if (parts.length > 1) {
                try {
                    top = Integer.parseInt(parts[1]);
                } catch (NumberFormatException ex) {
                    throw new CliException("topN must be an integer", ex);
                }
            }
            if (top <= 0) {
                throw new CliException("topN must be greater than zero");
            }
            List<Entity> res = store.findSimilar(id, top);
            System.out.println(gson.toJson(res.stream().map(Entity::getFields).collect(Collectors.toList())));
        } catch (CliException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw new CliException(e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to find similar with line: " + line, e);
            throw new CliException("unable to find similar entities: " + e.getMessage(), e);
        }
    }

    void createTransformFunction(String line) {
        try {
            int lb = line.indexOf('{');
            int rb = line.lastIndexOf('}');
            if (lb == -1 || rb == -1 || rb < lb) throw new CliException("expected body: create transform function { <expr> -> <field>; ... }");
            String body = line.substring(lb+1, rb).trim();
            transformFunction = new HashMap<>();
            for (String part : body.split("[;\n]")) {
                part = part.trim();
                if (part.isEmpty()) continue;
                int arrow = part.indexOf("->");
                if (arrow == -1) {
                    throw new CliException("each transform rule must use 'expr -> field' syntax");
                }
                String exprStr = part.substring(0, arrow).trim();
                String field = part.substring(arrow+2).trim();
                if (field.isEmpty()) {
                    throw new CliException("transform target field cannot be empty");
                }
                ValueExpression ve = parser.parseValueExpression(exprStr);
                transformFunction.put(field, ve);
            }
            if (transformFunction.isEmpty()) {
                throw new CliException("no transform expressions provided");
            }
            System.out.println("transform function created");
        } catch (CliException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw new CliException(e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create transform function with line: " + line, e);
            throw new CliException("unable to create transform function: " + e.getMessage(), e);
        }
    }

    void applyTransformFunction(String line) {
        try {
            String lower = line.toLowerCase(Locale.ROOT);
            int fromIdx = lower.indexOf("from set");
            if (fromIdx == -1) throw new CliException("usage: apply transform function from set <src> to <dest>");
            fromIdx += "from set".length();
            int toIdx = lower.indexOf("to", fromIdx);
            if (toIdx == -1) throw new CliException("usage: apply transform function from set <src> to <dest>");
            String setName = line.substring(fromIdx, toIdx).trim();
            String newSet = line.substring(toIdx + 2).trim();
            if (setName.isEmpty() || newSet.isEmpty()) {
                throw new CliException("source and destination set names are required");
            }
            if (transformFunction.isEmpty()) {
                throw new CliException("no transform function defined. Use 'create transform function' first");
            }
            Set<String> ids = sets.getOrDefault(setName, Collections.emptySet());
            if (ids.isEmpty()) {
                System.out.println("transformed 0");
                return;
            }
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
        } catch (CliException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw new CliException(e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to apply transform function with line: " + line, e);
            throw new CliException("unable to apply transform function: " + e.getMessage(), e);
        }
    }

    void persistSnapshot() {
        try {
            store.saveSnapshot();
            System.out.println("snapshot saved");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to persist snapshot", e);
            throw new CliException("unable to persist snapshot: " + e.getMessage(), e);
        }
    }

    Object getFieldValue(Entity e, String path) {
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

    private Map<String,Object> parseJsonDocument(String json, String context, boolean allowEmpty) {
        try {
            Map<String,Object> map = gson.fromJson(json, Map.class);
            if (map == null) {
                if (allowEmpty) {
                    return new HashMap<>();
                }
                throw new CliException(context + " cannot be empty");
            }
            return map;
        } catch (JsonSyntaxException ex) {
            throw new CliException(context + " contains invalid JSON. Wrap string values in double quotes.", ex);
        }
    }

    void printHelp() {
        printHelp(null);
    }

    void printHelp(String topic) {
        if (topic == null || topic.isBlank()) {
            System.out.println("Usage: <command> [arguments]\n");
            System.out.println("Available commands:");
            int width = HELP_ENTRIES.stream().mapToInt(h -> h.usage.length()).max().orElse(0);
            for (HelpEntry entry : HELP_ENTRIES) {
                System.out.printf(" %-" + width + "s  %s%n", entry.usage, entry.description);
            }
            System.out.println("\nType 'help <command>' for detailed information.");
            return;
        }

        String trimmed = topic.trim().toLowerCase(Locale.ROOT);
        for (HelpEntry entry : HELP_ENTRIES) {
            if (entry.matches(trimmed)) {
                System.out.println("Usage: " + entry.usage);
                System.out.println(entry.description);
                return;
            }
        }
        System.out.println("No help available for '" + topic + "'. Try one of: " +
                HELP_ENTRIES.stream().map(e -> e.usage.split("\\s+")[0]).distinct().collect(Collectors.joining(", ")));
    }
}
