package com.crux.query;

import com.crux.store.Entity;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Simple recursive descent parser for the filter grammar used by
 * the command line interface. The parser produces a tree of
 * {@link QueryExpression} objects which themselves act as the
 * evaluation AST.
 */
public class FilterParser {
    private static final Logger LOGGER = Logger.getLogger(FilterParser.class.getName());

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
            if (Character.isLetter(c) || Character.isDigit(c) || c == '_' ) {
                int start = pos;
                boolean sawLetter = Character.isLetter(c) || c == '_';
                pos++;
                while (pos < s.length()) {
                    char ch = s.charAt(pos);
                    if (Character.isLetterOrDigit(ch) || ch == '_' || ch == '.') {
                        if (Character.isLetter(ch) || ch == '_') {
                            sawLetter = true;
                        }
                        pos++;
                        continue;
                    }
                    if (ch == '-' && sawLetter && pos + 1 < s.length() && Character.isLetterOrDigit(s.charAt(pos + 1))) {
                        pos++;
                        continue;
                    }
                    break;
                }
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
        try {
            Lexer lexer = new Lexer(input);
            return parseOr(lexer);
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "Failed to parse filter: " + input, e);
            throw e;
        }
    }

    /** Parses a standalone value expression. */
    public ValueExpression parseValueExpression(String input) {
        try {
            Lexer lexer = new Lexer(input);
            return parseValueExpr(lexer);
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "Failed to parse value expression: " + input, e);
            throw e;
        }
    }

    private QueryExpression parseOr(Lexer l) {
        QueryExpression left = parseAnd(l);
        while (l.hasNext()) {
            String token = l.peek();
            if ("or".equalsIgnoreCase(token)) { l.next(); QueryExpression right = parseAnd(l); left = QueryExpression.or(left, right); }
            else break;
        }
        return left;
    }

    private QueryExpression parseAnd(Lexer l) {
        QueryExpression left = parseNot(l);
        while (l.hasNext()) {
            String token = l.peek();
            if ("and".equalsIgnoreCase(token)) { l.next(); QueryExpression right = parseNot(l); left = QueryExpression.and(left, right); }
            else break;
        }
        return left;
    }

    private QueryExpression parseNot(Lexer l) {
        String token = l.peek();
        if ("not".equalsIgnoreCase(token)) {
            l.next();
            QueryExpression expr = parsePrimary(l);
            return QueryExpression.not(expr);
        }
        return parsePrimary(l);
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
        if (op == null) {
            throw new RuntimeException("missing operator");
        }
        ValueExpression value = parseValueExpr(l);
        return comparisonExpression(field, op, value);
    }

    private QueryExpression comparisonExpression(String field, String op, ValueExpression value) {
        if ("contains".equalsIgnoreCase(op)) {
            if (value.isLiteral()) {
                Object literal = value.literalValue();
                if (literal != null) {
                    return QueryExpression.contains(field, literal.toString());
                }
            }
            return QueryExpression.fromPredicate(e -> {
                Object left = getFieldValue(e, field);
                Object right = value.eval(e);
                if (!(left instanceof String) || right == null) {
                    return false;
                }
                return ((String) left).toLowerCase(Locale.ROOT)
                        .contains(right.toString().toLowerCase(Locale.ROOT));
            });
        }
        if ("like".equalsIgnoreCase(op)) {
            if (value.isLiteral()) {
                Object literal = value.literalValue();
                if (literal != null) {
                    return QueryExpression.like(field, literal.toString());
                }
            }
            return QueryExpression.fromPredicate(e -> {
                Object left = getFieldValue(e, field);
                Object right = value.eval(e);
                if (!(left instanceof String) || right == null) {
                    return false;
                }
                return likeMatches(((String) left).toLowerCase(Locale.ROOT),
                        right.toString().toLowerCase(Locale.ROOT));
            });
        }
        String normalized = normalizeOperator(op);
        if (value.isLiteral()) {
            Object literal = value.literalValue();
            if (literal instanceof Comparable<?> comparable) {
                Optional<QueryExpression.Operator> operator = QueryExpression.Operator.fromSymbol(normalized);
                if (operator.isPresent()) {
                    return QueryExpression.field(field, operator.get(), (Comparable) comparable);
                }
            }
        }
        return QueryExpression.fromPredicate(e -> {
            Object left = getFieldValue(e, field);
            Object right = value.eval(e);
            return compare(left, right, normalized);
        });
    }

    private String normalizeOperator(String op) {
        if (op == null) return "==";
        return switch (op) {
            case "=" -> "==";
            case "==", "!=", ">", ">=", "<", "<=" -> op;
            default -> op;
        };
    }

    private boolean compare(Object l, Object r, String op) {
        op = normalizeOperator(op);
        if (l == null || r == null) {
            return switch (op) {
                case "==" -> Objects.equals(l, r);
                case "!=" -> !Objects.equals(l, r);
                default -> false;
            };
        }
        if (l instanceof Number || r instanceof Number) {
            double dl = ((Number) convertNumber(l)).doubleValue();
            double dr = ((Number) convertNumber(r)).doubleValue();
            return switch (op) {
                case "==" -> dl == dr;
                case "!=" -> dl != dr;
                case ">" -> dl > dr;
                case ">=" -> dl >= dr;
                case "<" -> dl < dr;
                case "<=" -> dl <= dr;
                default -> false;
            };
        }
        if (l instanceof Comparable c1 && r instanceof Comparable c2) {
            int cmp = c1.compareTo(c2);
            return switch (op) {
                case "==" -> cmp == 0;
                case "!=" -> cmp != 0;
                case ">" -> cmp > 0;
                case ">=" -> cmp >= 0;
                case "<" -> cmp < 0;
                case "<=" -> cmp <= 0;
                default -> false;
            };
        }
        return switch (op) {
            case "==" -> Objects.equals(l, r);
            case "!=" -> !Objects.equals(l, r);
            default -> false;
        };
    }

    private static Number convertNumber(Object o) {
        if (o instanceof Number n) return n;
        return Double.parseDouble(o.toString());
    }

    private boolean likeMatches(String text, String pattern) {
        if (text == null || pattern == null) {
            return false;
        }
        Pattern regex = Pattern.compile(toLikeRegex(pattern));
        return regex.matcher(text).matches();
    }

    private String toLikeRegex(String pattern) {
        StringBuilder sb = new StringBuilder();
        sb.append('^');
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case '%':
                    sb.append(".*");
                    break;
                case '_':
                    sb.append('.');
                    break;
                case '\\':
                    if (i + 1 < pattern.length()) {
                        sb.append(Pattern.quote(String.valueOf(pattern.charAt(++i))));
                    }
                    break;
                default:
                    if (".[]{}()*+-?^$|".indexOf(c) >= 0) {
                        sb.append('\\');
                    }
                    sb.append(c);
            }
        }
        sb.append('$');
        return sb.toString();
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
        // attempt numeric parsing, otherwise treat as string
        try {
            return ValueExpression.literal(Double.parseDouble(t));
        } catch (NumberFormatException ex) {
            return ValueExpression.literal(t);
        }
    }

    private static Object getFieldValue(Entity e, String path) {
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

        default boolean isLiteral() {
            return this instanceof LiteralValue;
        }

        default Object literalValue() {
            return this instanceof LiteralValue literal ? literal.value : null;
        }

        static ValueExpression literal(Object v) {
            return new LiteralValue(v);
        }

        static ValueExpression field(String f) {
            return new FieldValue(f);
        }

        static ValueExpression binary(ValueExpression l, String op, ValueExpression r) {
            return new BinaryValue(l, op, r);
        }
    }

    private static final class LiteralValue implements ValueExpression {
        private final Object value;

        private LiteralValue(Object value) {
            this.value = value;
        }

        @Override
        public Object eval(Entity entity) {
            return value;
        }
    }

    private static final class FieldValue implements ValueExpression {
        private final String path;

        private FieldValue(String path) {
            this.path = path;
        }

        @Override
        public Object eval(Entity entity) {
            return FilterParser.getFieldValue(entity, path);
        }
    }

    private static final class BinaryValue implements ValueExpression {
        private final ValueExpression left;
        private final String op;
        private final ValueExpression right;

        private BinaryValue(ValueExpression left, String op, ValueExpression right) {
            this.left = left;
            this.op = op;
            this.right = right;
        }

        @Override
        public Object eval(Entity entity) {
            Object lv = left.eval(entity);
            Object rv = right.eval(entity);
            if (lv instanceof Number || rv instanceof Number) {
                if (lv == null) lv = 0;
                if (rv == null) rv = 0;
                double a = ((Number) FilterParser.convertNumber(lv)).doubleValue();
                double b = ((Number) FilterParser.convertNumber(rv)).doubleValue();
                return switch (op) {
                    case "+" -> a + b;
                    case "-" -> a - b;
                    case "*" -> a * b;
                    case "/" -> a / b;
                    default -> 0d;
                };
            }
            if ("+".equals(op)) {
                return String.valueOf(lv) + rv;
            }
            return null;
        }
    }
}

