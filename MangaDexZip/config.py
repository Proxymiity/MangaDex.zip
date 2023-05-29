from pxyTools import JSONDict

config = JSONDict("config.json")


def _dict_put(d, keys, item):
    if "." in keys:
        key, rest = keys.split(".", 1)
        if key not in d:
            d[key] = {}
        _dict_put(d[key], rest, item)
    else:
        d[keys] = item


def _dict_get(d, keys):
    if "." in keys:
        key, rest = keys.split(".", 1)
        return _dict_get(d[key], rest)
    else:
        return d[keys]


for conf in config["additional_configurations"].copy():
    for k, v in JSONDict(conf).items():
        _dict_put(config, k, v)
