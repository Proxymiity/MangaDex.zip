from pxyTools import JSONDict

stats = JSONDict("stats.json")
volatile_stats = {
    "manga": 0,
    "manga_api": 0,
    "chapters": 0,
    "chapters_api": 0
}

if not stats:
    stats.update(**volatile_stats)


def add(stat, n=1):
    stats[stat] += n
    volatile_stats[stat] += n
