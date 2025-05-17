from collections import defaultdict

def compare_coin_mentions(range1, range2, popularity_filter="sentiment_count"):
    map1 = {coin['crypto']: coin for coin in range1}
    map2 = {coin['crypto']: coin for coin in range2}

    for coin in set(map1.keys()).union(map2.keys()):
        val1 = map1.get(coin, {}).get(popularity_filter, 0)
        val2 = map2.get(coin, {}).get(popularity_filter, 0)

        freq1 = freq2 = "equal"
        if val1 > val2:
            freq1, freq2 = "high", "low"
        elif val2 > val1:
            freq1, freq2 = "low", "high"

        if coin in map1:
            map1[coin]["popularity_rank"] = freq1
        if coin in map2:
            map2[coin]["popularity_rank"] = freq2

    return list(map1.values()), list(map2.values())


def get_prices_avg(df_filtered):
    market_caps = defaultdict(list)
    coins_prices = defaultdict(list)
    for _, row in df_filtered.iterrows():
        detected_coin = row["crypto"]
        coins_obj = row.get("coins", {})
        price_data = row.get("coins", {}).get("price_data", {})
        if isinstance(price_data, dict):
            symbols = coins_obj.get("symbols", [])
            for symbol in symbols:
                coin_price_data = price_data.get(symbol, {})
                if isinstance(coin_price_data, dict):
                    market_cap = coin_price_data.get("market_cap")
                    coin_price = coin_price_data.get("price")
                    if isinstance(market_cap, (int, float)):
                        market_caps[detected_coin].append(market_cap)
                        coins_prices[detected_coin].append(coin_price)
    
    
    avg_market_caps = {
            coin: (sum(values) / len(values)) if values!=0 else 0
            for coin, values in market_caps.items()
        }
    
    avg_coins_prices = {
            coin: (sum(values) / len(values)) if values!=0 else 0
            for coin, values in coins_prices.items()
        }
    
    return avg_market_caps, avg_coins_prices
