from collections import defaultdict
import json

with open("coins_mapping.json", "r", encoding="utf-8") as f:
    crypto_dict = json.load(f)

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
        detected_coins = row["crypto"]
        price_data = row.get("coins", {}).get("price_data", {})
        if isinstance(price_data, dict):
            for coin in detected_coins:
                symbol = crypto_dict[coin][0]
                coin_price_data = price_data.get(symbol, {})
                if isinstance(coin_price_data, dict):
                    market_cap = coin_price_data.get("market_cap")
                    coin_price = coin_price_data.get("price")
                    if isinstance(coin_price, (int, float)):
                            market_caps[coin] = market_cap
                            coins_prices[coin] = coin_price
    
    
    return market_caps, coins_prices
