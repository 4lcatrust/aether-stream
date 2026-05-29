package com.aetherstream.bronze.util;

/** Formats a rejected CDC record and the reason it was rejected as a JSON line. */
public final class DeadLetterUtil {
    private DeadLetterUtil() {}

    public static String format(String reason, Object record) {
        return "{\"reason\":\"" + escape(reason)
                + "\",\"record\":\"" + escape(record == null ? "" : record.toString())
                + "\"}";
    }

    private static String escape(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                default:   sb.append(c);
            }
        }
        return sb.toString();
    }
}
