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
