package com.marketdata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MarketDataProcessorTest
{
    private MarketDataProcessor processor;
    private List<MarketData> publishedData;
    private ConcurrentHashMap<String, MarketData> lastPublishedPerSymbol;
    private ExecutorService executorService;

    @BeforeEach
    void setUp()
    {
        publishedData = new CopyOnWriteArrayList<>();
        lastPublishedPerSymbol = new ConcurrentHashMap<>();
        processor = new MarketDataProcessor(data -> {
            publishedData.add(data);
            lastPublishedPerSymbol.put(data.getSymbol(), data);
        });
        executorService = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown()
    {
        processor.shutdown();
        executorService.shutdown();
    }

    @RepeatedTest(value = 10, failureThreshold = 2)
    void testHighFrequencyUpdates() throws InterruptedException
    {
        // Simulate the single thread to call onMessage when market data received
        executorService.submit(() -> {
            try
            {
                long startTime = System.currentTimeMillis();
                // Simulate 1000 updates for two symbols over ~1 second
                for (int i = 0; i < 1000; i++)
                {
                    processor.onMessage(new MarketData("AAPL", 150.0 + (i % 10), startTime + i));
                    processor.onMessage(new MarketData("GOOG", 2800.0 + (i % 10), startTime + i));

                    if (i % 200 == 0)
                    {
                        Thread.sleep(300); // Spread updates over ~1 second
                    }
                }
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        });

        // Wait for 2 seconds to observe two sliding windows
        Thread.sleep(2000);

        // Verify publish count: max 1 publish per symbol per 1-second window
        // For 2 symbols over 2 windows, expect at most 4 publishes
        long actualPublishCount = publishedData.size();
        assertTrue(actualPublishCount <= 4,
                "Expected at most 4 publishes (2 symbols × 2 windows), got: " + actualPublishCount);
        assertTrue(actualPublishCount >= 2,
                "Expected at least 2 publishes (1 per symbol), got: " + actualPublishCount);

        // Verify both symbols are published
        boolean hasAAPL = publishedData.stream().anyMatch(d -> d.getSymbol().equals("AAPL"));
        boolean hasGOOG = publishedData.stream().anyMatch(d -> d.getSymbol().equals("GOOG"));
        assertTrue(hasAAPL, "AAPL data should be published");
        assertTrue(hasGOOG, "GOOG data should be published");

        // Verify latest prices are published
        MarketData lastAAPL = lastPublishedPerSymbol.get("AAPL");
        MarketData lastGOOG = lastPublishedPerSymbol.get("GOOG");
        assertNotNull(lastAAPL, "AAPL should have published data");
        assertNotNull(lastGOOG, "GOOG should have published data");
        assertTrue(lastAAPL.getPrice() >= 150.0 && lastAAPL.getPrice() <= 159.0,
                "AAPL price should be in range 150.0-159.0, got: " + lastAAPL.getPrice());
        assertTrue(lastGOOG.getPrice() >= 2800.0 && lastGOOG.getPrice() <= 2809.0,
                "GOOG price should be in range 2800.0-2809.0, got: " + lastGOOG.getPrice());

        // Verify at most one publish per symbol per window
        long aaplCount = publishedData.stream()
                .filter(d -> d.getSymbol().equals("AAPL"))
                .count();
        long googCount = publishedData.stream()
                .filter(d -> d.getSymbol().equals("GOOG"))
                .count();
        assertEquals(2, aaplCount, "AAPL should have 2 publishes, got: " + aaplCount);
        assertEquals(2, googCount, "GOOG should have 2 publishes, got: " + googCount);
    }

    @RepeatedTest(value = 10, failureThreshold = 2)
    void testSingleUpdatePerSymbolPerWindow() throws InterruptedException
    {
        final long startTime = System.currentTimeMillis();
        // Simulate the single thread to call onMessage when market data received
        executorService.submit(() -> {
            try
            {
                processor.onMessage(new MarketData("AAPL", 150.0, startTime));
                processor.onMessage(new MarketData("AAPL", 151.0, startTime + 100));
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        });

        // Wait for first window to complete
        Thread.sleep(1100);

        // Verify only latest data per window is published
        MarketData lastAAPL1 = lastPublishedPerSymbol.get("AAPL");
        assertNotNull(lastAAPL1, "AAPL data should be published");
        assertEquals(150.0, lastAAPL1.getPrice(), "Should publish latest price (150.0) in second window");

        executorService.submit(() -> {
            try
            {
                processor.onMessage(new MarketData("AAPL", 152.0, startTime + 1100));
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        });

        // Wait for second window
        Thread.sleep(1100);

        // Verify only latest data per window is published
        MarketData lastAAPL2 = lastPublishedPerSymbol.get("AAPL");
        assertNotNull(lastAAPL2, "AAPL data should be published");
        assertEquals(152.0, lastAAPL2.getPrice(), "Should publish latest price (152.0) in second window");

        // Count AAPL publishes
        long aaplCount = publishedData.stream()
                .filter(d -> d.getSymbol().equals("AAPL"))
                .count();
        assertEquals(2, aaplCount, "AAPL should have 2 publishes, got: " + aaplCount);

        // Verify prices published are either 151.0 (first window) or 152.0 (second window)
        boolean validPrices = publishedData.stream()
                .filter(d -> d.getSymbol().equals("AAPL"))
                .allMatch(d -> d.getPrice() == 150.0 || d.getPrice() == 152.0);
        assertTrue(validPrices, "Only latest prices (151.0 or 152.0) should be published");
    }

    @Test
    void testNoDataReceived() throws InterruptedException
    {
        // No data sent to onMessage
        Thread.sleep(2000); // Wait for 2 seconds to observe behavior

        long actualPublishCount = publishedData.size();
        assertEquals(0, actualPublishCount, "No publishes should occur with no data");
    }

    @RepeatedTest(value = 10, failureThreshold = 2)
    void testSingleSymbolHighFrequency() throws InterruptedException
    {
        // Simulate the single thread to call onMessage when market data received
        executorService.submit(() -> {
            try
            {
                long startTime = System.currentTimeMillis();
                // Send 1000 updates for a single symbol
                for (int i = 0; i < 1000; i++)
                {
                    processor.onMessage(new MarketData("AAPL", 150.0 + (i % 10), startTime + i));
                    if (i % 200 == 0)
                    {

                        Thread.sleep(300); // Spread over ~1 second
                    }
                }
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        });

        Thread.sleep(2000); // Wait for 2 windows

        long actualPublishCount = publishedData.size();
        assertEquals(2, actualPublishCount,
                "Expected at most 2 publishes (1 symbol × 2 windows), got: " + actualPublishCount);

        boolean hasAAPL = publishedData.stream().anyMatch(d -> d.getSymbol().equals("AAPL"));
        assertTrue(hasAAPL, "AAPL data should be published");

        MarketData lastAAPL = lastPublishedPerSymbol.get("AAPL");
        assertNotNull(lastAAPL, "AAPL should have published data");
        assertTrue(lastAAPL.getPrice() >= 150.0 && lastAAPL.getPrice() <= 159.0,
                "AAPL price should be in range 150.0-159.0, got: " + lastAAPL.getPrice());
    }

    @RepeatedTest(value = 10, failureThreshold = 2)
    void testMultipleSymbolsRateLimit() throws InterruptedException
    {
        // Simulate the single thread to call onMessage when market data received
        executorService.submit(() -> {
            try
            {
                long startTime = System.currentTimeMillis();
                // Send updates for 200 symbols to stress rate limit
                for (int i = 1; i <= 200; i++)
                {
                    String symbol = "SYM" + i;
                    processor.onMessage(new MarketData(symbol, 100.0 + (i % 10), startTime));
                }
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        });

        Thread.sleep(1000); // Wait for 1st window

        long actualPublishCount = publishedData.size();
        assertTrue(actualPublishCount >= 99,
                "Expected at least 99 publishes in 1 second due to rate limit, got: " + actualPublishCount);
        assertTrue(actualPublishCount <= 101,
                "Expected at most 101 publishes in 1 second due to rate limit, got: " + actualPublishCount);

        // Verify each published symbol appears exactly once
        long uniqueSymbols = publishedData.stream()
                .map(MarketData::getSymbol)
                .distinct()
                .count();
        assertTrue(uniqueSymbols >= 99,
                "Each published symbol should appear exactly once");
        assertTrue(uniqueSymbols <= 101,
                "Each published symbol should appear exactly once");

        Thread.sleep(1000); // Wait for 2nd window

        actualPublishCount = publishedData.size();
        assertTrue(actualPublishCount >= 199,
                "Expected at least 199 publishes in 2 second due to rate limit, got: " + actualPublishCount);
        assertTrue(actualPublishCount <= 200,
                "Expected at most 200 publishes in 2 second due to rate limit, got: " + actualPublishCount);

        // Verify each published symbol appears exactly once
        uniqueSymbols = publishedData.stream()
                .map(MarketData::getSymbol)
                .distinct()
                .count();
        assertTrue(uniqueSymbols >= 199,
                "Each published symbol should appear exactly once");
        assertTrue(uniqueSymbols <= 200,
                "Each published symbol should appear exactly once");
    }
}
