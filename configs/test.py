def fib(number: int) -> int:
    if number == 0: return 0
    if number == 1: return 1
    return fib(number - 1) + fib(number - 2)


if __name__ == '__main__':
    fib(10)
