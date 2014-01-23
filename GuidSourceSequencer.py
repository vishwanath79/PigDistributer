
def guidsourcesequencer(input):
    output = []
    for rank, item in enumerate(input):
        output.append(tuple([rank] + list(item)))
    return output
