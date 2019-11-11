def interval_split(array, interval):
    """Функция разбивает список на интервалы заданной длины.
    Возвращает список списков, при чем длина подсписка равна 2,
    где первое значение подсписка - это первый элемент интервала,
    а второй элемемен подсписка - это последний элемент интервала.

    Пример:
    get_intervals([1,2,3,4,5,6,7], 3) => [[1,3], [4,6], [7]]
    get_intervals([1,2,3], 4) => [[1,3]]"""
    intervals = []
    iw, i = 0, 0
    l = len(array)
    for v in array:
        if i==0 or (i)%interval==0:
            intervals.append([v])
        if (i+1)%interval == 0 or (i+1) == l:
            intervals[iw].append(v)
            iw+=1
        i+=1
    return intervals
