

def overlap(li:list):
    out = [v for v in li[0] if v in li[1]]
    if len(li) > 2:
        out = overlap([out] + li[2:])
    return out

def flatten_one(li):
    """Reduces depth of li of by 1"""
    out_li = []
    for v1 in li:
        if isinstance(v1, list):
            _ = [out_li.append(v2) for v2 in v1]
        else:
            out_li.append(v1)
    return out_li

def flatten_full(li):
    #Flatten any lists inside
    out_li = []
    for v in li:
        if isinstance(v, list):
            flat_v = flatten_one(v)
            #Check for internal lists
            for v2 in flat_v:
                if isinstance(v2, list):
                    _ = [out_li.append(v3) for v3 in flatten(v2)]
                else:
                    out_li.append(v2)
        else:
            out_li.append(v)
    return out_li