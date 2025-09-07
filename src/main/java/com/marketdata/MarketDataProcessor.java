package com.marketdata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class MarketDataProcessor
{
    // Token bucket parameters
    private static final int MAX_PUBLISH_PER_SECOND = 100;
    private static final long WINDOW_SIZE_MS = 1000;

    private final Logger logger = Logger.getGlobal();

    private final LinkedBlockingQueue<MarketData> marketDataQueue;
    private final ScheduledExecutorService scheduler;
    private final Consumer<MarketData> publishHandler;
    // Track last publish time per symbol to enforce one update per window
    private final ConcurrentHashMap<String, Long> lastPublishTimePerSymbol;

    private volatile int availableTokens;
    private volatile long lastRefillTime;

    public MarketDataProcessor(Consumer<MarketData> publishHandler)
    {
        this.marketDataQueue = new LinkedBlockingQueue<>();
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.publishHandler = publishHandler;
        this.availableTokens = MAX_PUBLISH_PER_SECOND;
        this.lastPublishTimePerSymbol = new ConcurrentHashMap<>();
        this.lastRefillTime = 0L;
        startPublishing();
    }

    public void onMessage(MarketData data) throws InterruptedException
    {
        logger.info("onMessage: " + data);
        marketDataQueue.put(data);
    }

    private void publishAggregatedMarketData(MarketData data)
    {
        logger.info("publishAggregatedMarketData: " + data);
        publishHandler.accept(data);
    }

    // Refill tokens and clean up old timestamps
    private synchronized void refillTokensIfNeeded()
    {
        long currentTime = System.currentTimeMillis();
        // Refine token after 1-second
        boolean refill = currentTime - lastRefillTime > WINDOW_SIZE_MS;
        if (refill)
        {
            lastRefillTime = currentTime;
            availableTokens = MAX_PUBLISH_PER_SECOND;
            logger.info("Refill tokens: " + availableTokens);
        }
    }

    private synchronized boolean tryConsumeToken()
    {
        if (availableTokens >= 1)
        {
            availableTokens -= 1;
            logger.info("Consume 1 token, with number of tokens now: " + availableTokens);
            return true;
        }
        return false;
    }

    private void startPublishing()
    {
        // Fixed rate schedule task which is maximum to call 100 times publishAggregatedMarketData(), it separates
        // a fixed small slide with 10 milliseconds once. It results 100 times called per second.
        scheduler.scheduleAtFixedRate(() -> {
            try
            {
                refillTokensIfNeeded();

                if (availableTokens >= 1 && !marketDataQueue.isEmpty() && tryConsumeToken())
                {
                    while (!marketDataQueue.isEmpty())
                    {
                        MarketData data = marketDataQueue.take();
                        String symbol = data.getSymbol();
                        // Check if symbol was published in the last window
                        Long lastPublishTime = lastPublishTimePerSymbol.getOrDefault(symbol, 0L);
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastPublishTime > WINDOW_SIZE_MS)
                        {
                            publishAggregatedMarketData(data);
                            lastPublishTimePerSymbol.put(symbol, currentTime);
                            break;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.warning("Error in publishing: " + e.getMessage());
            }
        }, 0, 10, TimeUnit.MILLISECONDS);
    }

    public void shutdown()
    {
        scheduler.shutdown();
        try
        {
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS))
            {
                scheduler.shutdownNow();
            }
        }
        catch (InterruptedException e)
        {
            scheduler.shutdownNow();
        }
    }
}
