package io.logz.mdcwrapper;

import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MDCWrapper {

    private static ThreadLocal<Map<String, Stack<String>>> threadLocalMDCState = ThreadLocal.withInitial(HashMap::new);

    public static void withMDC(Map<String, String> mdcKeyValues, Runnable lambda) {
        withMDC(mdcKeyValues, () -> {
            lambda.run();
            return true;
        });
    }

    public static void withMDC(Map<String, String> mdcKeyValues, String keysPrefix, Runnable lambda) {
        withMDC(createMdcKeyValuesWithPrefix(mdcKeyValues, keysPrefix), lambda);
    }

    public static void withMDCWithTimer(Map<String, String> mdcKeyValues, Runnable lambda, Runnable logging) {
        withMDCWithTimer(mdcKeyValues, () -> {
            lambda.run();
            return true;
        }, logging);
    }

    public static <T> T withMDC(Map<String, String> mdcKeyValues, Supplier<T> lambda) {
        try {
            mdcKeyValues.forEach(MDCWrapper::mdcPut);
            return lambda.get();
        } finally {
            mdcKeyValues.keySet().forEach(MDCWrapper::mdcRemove);
        }
    }

    public static <T> T withMDC(Map<String, String> mdcKeyValues, String keysPrefix, Supplier<T> lambda) {
        return withMDC(createMdcKeyValuesWithPrefix(mdcKeyValues, keysPrefix), lambda);
    }

    private static Map<String, String> createMdcKeyValuesWithPrefix(Map<String, String> mdcKeyValues, String keysPrefix) {
        return mdcKeyValues.entrySet().stream()
                .collect(Collectors.toMap(entry -> keysPrefix + entry.getKey(), Map.Entry::getValue));
    }

    public static <T> T withMDCWithTimer(Map<String, String> mdcKeyValues, Supplier<T> lambda, Runnable logging) {
        return withMDCWithTimer(mdcKeyValues, lambda, result -> logging.run());
    }

    public static <T> T withMDCWithTimer(Map<String, String> mdcKeyValues, Supplier<T> lambda, Consumer<T> logging) {
        return withMDC(mdcKeyValues, () -> {
            long startTime = System.currentTimeMillis();
            T value = lambda.get();
            try {
                mdcPut("operationTimeMs", String.valueOf(System.currentTimeMillis() - startTime));
                logging.accept(value);
            } finally {
                mdcRemove("operationTimeMs");
            }
            return value;
        });
    }

    private static void mdcPut(String key, String value) {
        Map<String, Stack<String>> mdcState = threadLocalMDCState.get();
        Stack<String> values = mdcState.computeIfAbsent(key, theKey -> new Stack<>());
        values.push(value);
        MDC.put(key, value);
    }

    private static void mdcRemove(String key) {
        Map<String, Stack<String>> mdcState = threadLocalMDCState.get();
        Stack<String> values = mdcState.get(key);
        values.pop();
        if(values.empty()) {
            MDC.remove(key);
            mdcState.remove(key);
        } else {
            MDC.put(key, values.peek());
        }
    }
}

