# 这是正确、可导入的函数
def calc_10year_percentile(history, current):
    arr = sorted(history)
    cnt = 0
    for v in arr:
        if v <= current:
            cnt += 1
    return round(cnt / len(arr) * 100, 2)