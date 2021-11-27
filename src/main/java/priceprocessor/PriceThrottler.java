package priceprocessor;

import java.util.*;
import java.util.concurrent.*;

public class PriceThrottler implements PriceProcessor {
    private final Set<PriceProcessor> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Map<String, Map.Entry<Double, Integer>> pairToPriceVersion = new ConcurrentHashMap<>();
    private final BlockingQueue<String> pairsPriceToHandle = new LinkedBlockingQueue<String>();
    private final ExecutorService pool = Executors.newCachedThreadPool();
    {
        startInputHandlerAsync();
    }

    private void startInputHandlerAsync() {
        Executors.newSingleThreadExecutor().execute(
                () -> {
                    try {
                        while (true) {
                            sumbitPairToWorkers(pairsPriceToHandle.take());
                        }
                    } catch (InterruptedException e) {
                        // say goodbye
                    }
                });
    }

    private void sumbitPairToWorkers(String pair) {
        Executors.newSingleThreadExecutor().execute(
                () -> {
                    Integer originPriceVersion = pairToPriceVersion.get(pair).getValue();
                    notifySubscribers(pair,originPriceVersion);
                }
        );
    }

    private void notifySubscribers(String pair, Integer originPriceVersion) {
        listeners.parallelStream().forEach(
                priceProcessor -> {
                    if(Objects.equals(originPriceVersion, pairToPriceVersion.get(pair).getValue())){
                        priceProcessor.onPrice(pair,pairToPriceVersion.get(pair).getKey());
                    }
                }
        );
    }

    @Override
    public void onPrice(String ccyPair, double rate) {
        System.out.println("new price for: " + ccyPair + " : " + rate);
        pairToPriceVersion.compute(ccyPair, (p, priceToVersion) -> {
            if(priceToVersion==null){
                return new AbstractMap.SimpleEntry<>(rate,1);
            }
            return new AbstractMap.SimpleEntry<>(rate,priceToVersion.getValue() + 1);
        });
        pairsPriceToHandle.add(ccyPair);
    }

    @Override
    public void subscribe(PriceProcessor priceProcessor) {
        listeners.add(priceProcessor);
        System.out.println(priceProcessor + " added");
    }

    @Override
    public void unsubscribe(PriceProcessor priceProcessor) {
        listeners.remove(priceProcessor);
        System.out.println(priceProcessor + " removed");
    }

    public void shutDown() {
        pool.shutdown();
    }
}
