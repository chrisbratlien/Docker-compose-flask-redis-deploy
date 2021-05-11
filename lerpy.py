def clamp01(t):
    t = max(0, t)
    t = min(t, 1)
    return t


def lerp(a, b, t, clamp=False):
    if clamp:
        t = clamp01(t)
    result = (1-t) * a + t * b
    return result


def invlerp(a, b, v, clamp=False):
    t = (v - a) / (b - a)
    if not clamp:
        return t
    t = clamp01(t)
    return t


def remap(in_min, in_max, out_min, out_max, v, clamp=False):
    t = invlerp(in_min, in_max, v, clamp)
    result = lerp(out_min, out_max, t)
    # don't reclamp here.
    return result
