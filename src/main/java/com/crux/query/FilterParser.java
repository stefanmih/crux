package com.crux.query;

import com.crux.store.Entity;

import java.util.*;

/**
 * Simple recursive descent parser for the filter grammar used by
 * the command line interface. The parser produces a tree of
 * {@link QueryExpression} objects which themselves act as the
 * evaluation AST.
 */
public class FilterParser {

    private static class Lexer {
        private final String s;
        private int pos = 0;

        Lexer(String s) { this.s = s; }

        boolean hasNext() { skipWs(); return pos < s.length(); }

        String peek() { int p = pos; String t = next(); pos = p; return t; }

        String next() {
            skipWs();
            if (pos >= s.length()) return null;
            char c = s.charAt(pos);
            if (Character.isLetter(c) || c == '_' ) {
                int start = pos;
                while (pos < s.length() &&
                        (Character.isLetterOrDigit(s.charAt(pos)) || s.charAt(pos)=='_' || s.charAt(pos)=='.')) pos++;
                return s.substring(start, pos);
            }
            if (Character.isDigit(c)) {
                int start = pos;
                while (pos < s.length() && (Character.isDigit(s.charAt(pos)) || s.charAt(pos)=='.')) pos++;
                return s.substring(start, pos);
            }
            if (c == '"' || c=='\'') {
                char quote = c; pos++;
                int start = pos;
                while (pos < s.length() && s.charAt(pos)!=quote) pos++;
                String str = s.substring(start, pos);
                if (pos < s.length()) pos++;
                return quote+str+quote;
            }
            if (c=='{' || c=='}' || c=='(' || c==')' || c=='[' || c==']') {
                pos++; return Character.toString(c);
            }
            if (c=='&' || c=='+' || c=='-' || c=='*' || c=='/') {
                pos++; return Character.toString(c);
            }
            // operators
            if (c=='=' && pos+1<s.length() && s.charAt(pos+1)=='=') { pos+=2; return "=="; }
            if (c=='!' && pos+1<s.length() && s.charAt(pos+1)=='=') { pos+=2; return "!="; }
            if (c=='>' && pos+1<s.length() && s.charAt(pos+1)=='=') { pos+=2; return ">="; }
            if (c=='<' && pos+1<s.length() && s.charAt(pos+1)=='=') { pos+=2; return "<="; }
            if (c=='>' || c=='<') { pos++; return Character.toString(c); }
            // comma or colon
            if (c==':' || c==',') { pos++; return Character.toString(c); }
            // otherwise single char token
            pos++; return Character.toString(c);
        }

        private void skipWs() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) pos++;
        }
    }

    public QueryExpression parse(String input) {
        Lexer lexer = new Lexer(input);
        return parseOr(lexer);
    }

    /** Parses a standalone value expression. */
    public ValueExpression parseValueExpression(String input) {
        Lexer lexer = new Lexer(input);
        return parseValueExpr(lexer);
    }

    private QueryExpression parseOr(Lexer l) {
        QueryExpression left = parseAnd(l);
        while (l.hasNext()) {
            String token = l.peek();
            if ("or".equals(token)) { l.next(); QueryExpression right = parseAnd(l); left = QueryExpression.or(left, right); }
            else break;
        }
        return left;
    }

    private QueryExpression parseAnd(Lexer l) {
        QueryExpression left = parseNot(l);
        while (l.hasNext()) {
            String token = l.peek();
            if ("and".equals(token)) { l.next(); QueryExpression right = parseNot(l); left = QueryExpression.and(left, right); }
            else break;
        }
        return left;
    }

    private QueryExpression parseNot(Lexer l) {
        String token = l.peek();
        if ("not".equals(token)) {
            l.next();
            QueryExpression expr = parsePrimary(l);
            return entityNot(expr);
        }
        return parsePrimary(l);
    }

    private QueryExpression entityNot(QueryExpression expr) {
        return (indexes, store) -> {
            Set<String> all = new HashSet<>();
            for (Entity e : store.findAll()) {
                all.add(e.getId());
            }
            all.removeAll(expr.evaluate(indexes, store));
            return all;
        };
    }

    private QueryExpression parsePrimary(Lexer l) {
        String token = l.peek();
        if ("(".equals(token)) {
            l.next();
            QueryExpression e = parseOr(l);
            if (!")".equals(l.next())) throw new RuntimeException("Expected )");
            return e;
        }
        if ("{".equals(token)) {
            return parseJsonFilter(l);
        }
        return parseComparison(l);
    }

    private QueryExpression parseJsonFilter(Lexer l) {
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        while (l.hasNext()) {
            String t = l.next();
            sb.append(t);
            if ("{".equals(t)) depth++;
            if ("}".equals(t)) { depth--; if (depth==0) break; }
        }
        String json = sb.toString();
        com.google.gson.Gson gson = new com.google.gson.Gson();
        Map<String,Object> map = gson.fromJson(json, Map.class);
        QueryExpression result = null;
        for (var e : map.entrySet()) {
            QueryExpression cmp = comparisonExpression(e.getKey(), "==", ValueExpression.literal(e.getValue()));
            result = result==null? cmp : QueryExpression.and(result, cmp);
        }
        return result==null ? (i,s)->new HashSet<>() : result;
    }

    private QueryExpression parseComparison(Lexer l) {
        String field = l.next();
        String op = l.next();
        ValueExpression value = parseValueExpr(l);
        return comparisonExpression(field, op, value);
    }

    private QueryExpression comparisonExpression(String field, String op, ValueExpression value) {
        return (indexes, store) -> {
            Set<String> out = new HashSet<>();
            for (Entity e : store.findAll()) {
                Object left = getFieldValue(e, field);
                Object right = value.eval(e);
                if (compare(left, right, op)) {
                    out.add(e.getId());
                }
            }
            return out;
        };
    }

    private boolean compare(Object l, Object r, String op) {
        if (l == null || r == null) {
            return "==".equals(op) ? Objects.equals(l,r) : "!=".equals(op) && !Objects.equals(l,r);
        }
        if (l instanceof Number || r instanceof Number) {
            double dl = ((Number) convertNumber(l)).doubleValue();
            double dr = ((Number) convertNumber(r)).doubleValue();
            return switch (op) {
                case "==" -> dl==dr;
                case "!=" -> dl!=dr;
                case ">" -> dl>dr;
                case ">=" -> dl>=dr;
                case "<" -> dl<dr;
                case "<=" -> dl<=dr;
                default -> false;
            };
        }
        if (l instanceof Comparable c1 && r instanceof Comparable c2) {
            int cmp = c1.compareTo(c2);
            return switch (op) {
                case "==" -> cmp==0;
                case "!=" -> cmp!=0;
                case ">" -> cmp>0;
                case ">=" -> cmp>=0;
                case "<" -> cmp<0;
                case "<=" -> cmp<=0;
                default -> false;
            };
        }
        return false;
    }

    private Number convertNumber(Object o) {
        if (o instanceof Number n) return n;
        return Double.parseDouble(o.toString());
    }

    private ValueExpression parseValueExpr(Lexer l) {
        return parseAdd(l);
    }

    private ValueExpression parseAdd(Lexer l) {
        ValueExpression left = parseMul(l);
        while (l.hasNext()) {
            String t = l.peek();
            if ("+".equals(t) || "-".equals(t)) {
                l.next();
                ValueExpression right = parseMul(l);
                left = ValueExpression.binary(left, t, right);
            } else break;
        }
        return left;
    }

    private ValueExpression parseMul(Lexer l) {
        ValueExpression left = parseUnary(l);
        while (l.hasNext()) {
            String t = l.peek();
            if ("*".equals(t) || "/".equals(t)) {
                l.next();
                ValueExpression right = parseUnary(l);
                left = ValueExpression.binary(left, t, right);
            } else break;
        }
        return left;
    }

    private ValueExpression parseUnary(Lexer l) {
        String t = l.peek();
        if ("-".equals(t)) {
            l.next();
            ValueExpression v = parseUnary(l);
            return ValueExpression.binary(ValueExpression.literal(0), "-", v);
        }
        return parseTerm(l);
    }

    private ValueExpression parseTerm(Lexer l) {
        String t = l.next();
        if ("(".equals(t)) {
            ValueExpression v = parseValueExpr(l);
            if (!")".equals(l.next())) throw new RuntimeException("Expected )");
            return v;
        }
        if ("&".equals(t)) {
            String f = l.next();
            return ValueExpression.field(f);
        }
        if (t.startsWith("\"") || t.startsWith("'")) {
            return ValueExpression.literal(t.substring(1, t.length()-1));
        }
        if ("true".equalsIgnoreCase(t) || "false".equalsIgnoreCase(t)) {
            return ValueExpression.literal(Boolean.parseBoolean(t));
        }
        // number
        return ValueExpression.literal(Double.parseDouble(t));
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

    /** Value expression node. */
    public interface ValueExpression {
        Object eval(Entity entity);

        static ValueExpression literal(Object v) { return e -> v; }

        static ValueExpression field(String f) { return e -> new FilterParser().getFieldValue(e, f); }

        static ValueExpression binary(ValueExpression l, String op, ValueExpression r) {
            return e -> {
                Object lv = l.eval(e);
                Object rv = r.eval(e);
                if (lv instanceof Number || rv instanceof Number) {
                    double a = ((Number)new FilterParser().convertNumber(lv)).doubleValue();
                    double b = ((Number)new FilterParser().convertNumber(rv)).doubleValue();
                    return switch (op) {
                        case "+" -> a + b;
                        case "-" -> a - b;
                        case "*" -> a * b;
                        case "/" -> a / b;
                        default -> 0;
                    };
                }
                if ("+".equals(op)) {
                    return String.valueOf(lv) + rv;
                }
                return null;
            };
        }
    }
}

