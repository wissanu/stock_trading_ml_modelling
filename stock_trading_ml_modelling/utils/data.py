

def overlap(li:list):
    out = [v for v in li[0] if v in li[1]]
    if len(li) > 2:
        out = overlap([out] + li[2:])
    return out