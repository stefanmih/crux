package com.crux.cli;

import com.crux.query.QueryExpression;
import com.crux.store.Entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Parses command line input into a {@link Command} AST that can be
 * executed by {@link CommandLine}. This replaces the previous
 * string matching approach which relied heavily on {@code startsWith}.
 */
public class CommandParser {

    /** Command node which can be executed against a {@link CommandLine}. */
    interface Command {
        void execute(CommandLine cli);
    }

    /** Tokenizer capable of handling quoted strings and nested braces. */
    private static class Tokenizer {
        private final String s;
        private int pos = 0;
        Tokenizer(String s) { this.s = s; }

        boolean hasNext() {
            skipWs();
            return pos < s.length();
        }

        String next() {
            skipWs();
            if (pos >= s.length()) return null;
            char c = s.charAt(pos);
            if (c=='"' || c=='\'') {
                char q=c; pos++;
                int start=pos;
                while (pos<s.length() && s.charAt(pos)!=q) pos++;
                String str = s.substring(start,pos);
                if (pos<s.length()) pos++;
                return q + str + q;
            }
            if (c=='{' ) {
                int level=0; int start=pos;
                do {
                    char cc=s.charAt(pos);
                    if (cc=='{') level++;
                    else if (cc=='}') level--;
                    pos++;
                } while(pos<s.length() && level>0);
                return s.substring(start,pos);
            }
            if (c=='[') {
                int level=0; int start=pos;
                do {
                    char cc=s.charAt(pos);
                    if (cc=='[') level++;
                    else if (cc==']') level--;
                    pos++;
                } while(pos<s.length() && level>0);
                return s.substring(start,pos);
            }
            int start=pos;
            while (pos<s.length() && !Character.isWhitespace(s.charAt(pos))) pos++;
            return s.substring(start,pos);
        }

        String rest() {
            return s.substring(pos).trim();
        }

        private void skipWs() {
            while (pos<s.length() && Character.isWhitespace(s.charAt(pos))) pos++;
        }
    }

    /** Parses a command line into an executable command. */
    Command parse(String line) {
        Tokenizer t = new Tokenizer(line);
        if (!t.hasNext()) throw new RuntimeException("empty command");
        String first = t.next().toLowerCase(Locale.ROOT);
        return switch (first) {
            case "add" -> parseAdd(t);
            case "delete" -> parseDelete(t);
            case "update" -> parseUpdate(t);
            case "get" -> parseGet(t);
            case "generate" -> parseGenerate(t);
            case "find" -> parseFind(t);
            case "create" -> parseCreate(t);
            case "apply" -> parseApply(t);
            case "show" -> parseShow(t);
            case "help" -> cli -> cli.printHelp();
            default -> throw new RuntimeException("unknown command");
        };
    }

    private Command parseAdd(Tokenizer t) {
        String second = t.next();
        if (!"entity".equalsIgnoreCase(second)) {
            throw new RuntimeException("expected 'entity'");
        }
        String rest = t.rest();
        return cli -> cli.addEntity(rest);
    }

    private Command parseDelete(Tokenizer t) {
        String second = t.next();
        if (!"entity".equalsIgnoreCase(second)) {
            throw new RuntimeException("expected 'entity'");
        }
        String id = t.rest();
        return cli -> {
            cli.store.delete(id);
            for (var s : cli.sets.values()) s.remove(id);
            System.out.println("deleted " + id);
        };
    }

    private Command parseUpdate(Tokenizer t) {
        String second = t.next();
        if (!"entities".equalsIgnoreCase(second)) {
            throw new RuntimeException("expected 'entities'");
        }
        String third = t.next();
        if (!"where".equalsIgnoreCase(third)) {
            throw new RuntimeException("expected 'where'");
        }
        List<String> filterTokens = new ArrayList<>();
        while (t.hasNext()) {
            String tok = t.next();
            if ("set".equalsIgnoreCase(tok)) {
                String json = t.rest();
                String filter = String.join(" ", filterTokens);
                String line = "update entities where " + filter + " set " + json;
                return cli -> cli.updateEntities(line);
            }
            filterTokens.add(tok);
        }
        throw new RuntimeException("missing set clause");
    }

    private Command parseGet(Tokenizer t) {
        String second = t.next();
        if ("entities".equalsIgnoreCase(second)) {
            String using = t.next();
            String filterTok = t.next();
            if (!"using".equalsIgnoreCase(using) || !"filter".equalsIgnoreCase(filterTok)) {
                throw new RuntimeException("expected 'using filter'");
            }
            String expr = t.rest();
            return cli -> {
                QueryExpression q = cli.parser.parse(expr);
                List<Entity> res = cli.store.query(q);
                System.out.println(cli.gson.toJson(res.stream().map(Entity::getFields).collect(Collectors.toList())));
            };
        }
        if ("field".equalsIgnoreCase(second)) {
            String field = t.next();
            String from = t.next();
            if (!"from".equalsIgnoreCase(from)) {
                throw new RuntimeException("expected 'from'");
            }
            String id = t.next();
            return cli -> {
                Entity e = cli.store.get(id);
                if (e == null) { System.out.println("null"); return; }
                Object val = cli.getFieldValue(e, field);
                System.out.println(cli.gson.toJson(val));
            };
        }
        if ("some".equalsIgnoreCase(second)) {
            String token = t.hasNext() ? t.next() : null;
            int n = 5;
            if (token != null && token.startsWith("[")) {
                String inside = token.substring(1, token.length() - 1).trim();
                if (!inside.isEmpty()) n = Integer.parseInt(inside);
            }
            final int count = n;
            return cli -> {
                List<Map<String,Object>> result = new ArrayList<>();
                int i = 0;
                for (Entity e : cli.store.findAll()) {
                    if (i++ >= count) break;
                    result.add(e.getFields());
                }
                System.out.println(cli.gson.toJson(result));
            };
        }
        throw new RuntimeException("unknown get command");
    }

    private Command parseGenerate(Tokenizer t) {
        String rest = t.rest();
        return cli -> cli.generate("generate " + rest);
    }

    private Command parseFind(Tokenizer t) {
        String second = t.next();
        if ("similar".equalsIgnoreCase(second)) {
            String rest = t.rest();
            return cli -> cli.findSimilar("find similar " + rest);
        }
        throw new RuntimeException("unknown find command");
    }

    private Command parseCreate(Tokenizer t) {
        String second = t.next();
        if ("transform".equalsIgnoreCase(second)) {
            String third = t.next();
            if (!"function".equalsIgnoreCase(third)) {
                throw new RuntimeException("expected 'function'");
            }
            String rest = t.rest();
            return cli -> cli.createTransformFunction("create transform function " + rest);
        }
        throw new RuntimeException("unknown create command");
    }

    private Command parseApply(Tokenizer t) {
        String second = t.next();
        if ("transform".equalsIgnoreCase(second)) {
            String third = t.next();
            if (!"function".equalsIgnoreCase(third)) {
                throw new RuntimeException("expected 'function'");
            }
            String rest = t.rest();
            return cli -> cli.applyTransformFunction("apply transform function " + rest);
        }
        throw new RuntimeException("unknown apply command");
    }

    private Command parseShow(Tokenizer t) {
        String second = t.next();
        if ("history".equalsIgnoreCase(second)) {
            String id = t.next();
            return cli -> System.out.println(cli.gson.toJson(cli.store.getHistory(id)));
        }
        throw new RuntimeException("unknown show command");
    }
}
