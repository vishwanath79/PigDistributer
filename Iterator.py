@outputSchema("(op:chararray,pg:int,zip:chararray,count:int)")
def iterator(op,pg,zip,max):
    res = []
    x = 1
    while x <= max:
		res.append(x)
		x = x + 1
    return (op,pg,zip,res)
