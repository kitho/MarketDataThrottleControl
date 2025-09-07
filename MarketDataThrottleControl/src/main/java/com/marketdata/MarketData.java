package com.marketdata;

public class MarketData
{
    private final String symbol;
    private final double price;
    private final long updateTime;

    public MarketData(String symbol, double price, long updateTime)
    {
        this.symbol = symbol;
        this.price = price;
        this.updateTime = updateTime;
    }

    public String getSymbol()
    {
        return symbol;
    }

    public double getPrice()
    {
        return price;
    }

    public long getUpdateTime()
    {
        return updateTime;
    }

    @Override
    public String toString()
    {
        return "Symbol: " + symbol + ", Price: " + price + ", UpdateTime: " + updateTime;
    }
}
