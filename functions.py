import sys


def stable_decimal_places(one):
    """Преобразование вещественных чисел"""
    if isinstance(one, float):
        return f'{one:.10f}'
    return str(one)


def printProgressBar (iteration, total):
    length = 50
    percent = str(round((100 * (iteration / float(total))), 1))
    filled_length = int(length * iteration // total)
    bar = '█' * filled_length + '-' * (length - filled_length)
    sys.stdout.write(f'\rCollecting data |{bar}| {percent}% Complete')
    sys.stdout.flush()
    if iteration == total:
        print()