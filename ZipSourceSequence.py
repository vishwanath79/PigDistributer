@outputSchema("(operation:chararray,zip:chararray,pg:int,count:int)")
def zipsourcesequence(operation,zip,pg,max):
    res = []
    x = 1
    while x <= max:
		res.append(x)
		x = x + 1
    return (operation,zip,pg,res)
