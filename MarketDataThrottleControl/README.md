Assumptions

1. MarketData Structure: The MarketData class contains symbol (String), price (double), and updateTime (long, as milliseconds).
2. Sliding Window: The sliding window for rate limiting and per-symbol updates is 1 second (1000ms).
3. Token Bucket Parameters: The bucket has a capacity of 100 tokens, refilled at a rate of 100 tokens per second (0.1 tokens per millisecond), to enforce the limit of 100 publishes per second.
4. Data Storage: In-memory storage using a ConcurrentHashMap to hold the latest MarketData per symbol, ensuring thread-safety for single-threaded onMessage calls.
5. Time Tracking: Using System.currentTimeMillis() for token refills and to enforce one update per symbol per window.
6. Thread Safety: The Token Bucket methods (refillTokens, tryConsumeToken) are synchronized to ensure thread-safe token management, though onMessage is called by a single thread.
7. Per-Symbol Update Tracking: A ConcurrentHashMap tracks the last publish time per symbol to enforce one update per symbol per 1-second window.
8. No External Dependencies: The implementation uses only Java standard library (java.util.concurrent) for simplicity and compatibility.
9. Publishing Frequency: The publishing loop runs every 10ms to check for available tokens and data, balancing responsiveness and CPU usage.

Solution Strategy

- Token Bucket for Rate Limiting: Implement a Token Bucket Algorithm to limit publishes to 100 per second:
  - Maintain a bucket with a capacity of 100 tokens, refilled at 0.1 tokens per millisecond based on elapsed time.
  - Each publish consumes one token, checked via a synchronized tryConsumeToken method.
  - Refill tokens periodically using refillTokens, ensuring the bucket never exceeds 100 tokens to prevent bursting beyond the rate limit.
- Per-Symbol Update Control: Use a ConcurrentHashMap to store the last publish time per symbol, ensuring each symbol is published at most once per 1-second sliding window by checking the time since the last publish.
- Latest Data Storage: Use a ConcurrentHashMap to store the latest MarketData per symbol, overwriting older data with new updates to ensure the most recent data is published.
- Periodic Publishing: Use a ScheduledExecutorService to run a publishing task every 10ms, which:
  - Refills tokens based on elapsed time.
  - Checks for available tokens and unpublished data.
  - Publishes the latest MarketData for each eligible symbol (not published in the last 1000ms), consuming a token per publish.
- Removes published data from the map to prevent re-publishing until new updates arrive.
- Thread Safety: Ensure thread-safety for token management with synchronized methods and use ConcurrentHashMap for data and publish time storage, sufficient for single-threaded onMessage calls.
- Testability: Provide a constructor with a Consumer<MarketData> to capture published data for testing, allowing JUnit tests to verify throttling and per-symbol update rules.

=====================================================================================================================

The Token Bucket Algorithm is a rate-limiting algorithm commonly used in computer networking and system design to control the rate at which requests or data packets are processed. In Java, this algorithm can be implemented to manage API requests, network traffic, or other resource access.
How it works:

    Tokens:
    The algorithm uses "tokens" to represent permission to perform an action (e.g., send a data packet, process an API request).

Bucket:
These tokens are stored in a conceptual "bucket" with a fixed maximum capacity.
Refill Rate:
Tokens are added to the bucket at a constant, predefined rate. This rate determines the average allowed throughput.
Consumption:
When an action needs to be performed, it attempts to "consume" one or more tokens from the bucket.
Decision:

    If enough tokens are available, the action is allowed, and the corresponding number of tokens are removed from the bucket.

If insufficient tokens are available, the action is either denied (rejected) or queued/delayed until tokens become available.

Bursting:
The ability to store tokens up to the bucket's capacity allows for short bursts of activity, as accumulated tokens can be used quickly.

Key Parameters:

    Bucket Capacity (or Burst Size):
    The maximum number of tokens the bucket can hold. This determines the maximum burst of activity allowed.

Refill Rate (or Token Generation Rate):
The rate at which tokens are added to the bucket (e.g., tokens per second). This determines the long-term average rate. 
