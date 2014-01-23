@outputSchema("values:bag{t:tuple(key, value)}")
def bag_of_tuples(map):
    return map.items()
