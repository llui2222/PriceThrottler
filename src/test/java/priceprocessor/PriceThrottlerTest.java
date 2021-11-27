package priceprocessor;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class PriceThrottlerTest {

    private static final String[] PAIRS = {"EURUSD","EURRUB","USDRUB"};

    @Test
    public void process_3_processors() throws InterruptedException {
        int quantity= 3;
        PriceThrottler priceThrottler = new PriceThrottler();
        Map<PriceProcessor,Boolean> processors = createProcessors(quantity);
        Thread priceGenerator = createPriceGenerator(priceThrottler);
        priceGenerator.start();
        addRemoveProcessorsSync(priceThrottler, processors);
        priceGenerator.interrupt();
        priceThrottler.shutDown();
    }

    private void addRemoveProcessorsSync(PriceThrottler priceThrottler,
                                         Map<PriceProcessor,Boolean> processors) throws InterruptedException {
        for (int i = 0; i <30; i++) {
            Map.Entry<PriceProcessor,Boolean> randomEntry =
                    (Map.Entry<PriceProcessor, Boolean>) processors.entrySet()
                            .toArray()[ThreadLocalRandom.current().nextInt(processors.size())];
            if(randomEntry.getValue()){ // added
                priceThrottler.unsubscribe(randomEntry.getKey());
                randomEntry.setValue(false);
            }else{
                priceThrottler.subscribe(randomEntry.getKey());
                randomEntry.setValue(true);
            }
            Thread.sleep(ThreadLocalRandom.current().nextInt(1000));
        }
    }

    private Thread createPriceGenerator(PriceThrottler priceThrottler) {
        return new Thread(() -> {
            while (!Thread.interrupted()) {
                priceThrottler.onPrice(
                        PAIRS[ThreadLocalRandom.current().nextInt(PAIRS.length)],
                        Math.round(ThreadLocalRandom.current().nextDouble(10d) * 100) / 100d
                );
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(1_000));
                } catch (InterruptedException e) {
                }
            }
        });
    }

    private Map<PriceProcessor, Boolean> createProcessors(int quantity) {
        Map<PriceProcessor, Boolean> processors = new HashMap<>();
        for (int i = 0; i < quantity; i++) {
            processors.put(createProcessor(i),false);
        }
        return processors;
    }

    private PriceProcessor createProcessor(int i) {
        return new PriceProcessor() {
            @Override
            public void onPrice(String ccyPair, double rate) {
                System.out.println(this + " processing " + ccyPair + " " + rate);
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(1_000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void subscribe(PriceProcessor priceProcessor) {

            }

            @Override
            public void unsubscribe(PriceProcessor priceProcessor) {

            }

            @Override
            public String toString() {
                return "PriceProcessor" + i;
            }
        };
    }
}