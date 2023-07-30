def print_args(*args):
    for arg in args:
        print(arg)

print_args('Hello', 'world', '!', 123)
# Output: Hello
#         world
#         !
#         123



def print_kwargs(**kwargs):
    print(kwargs.items())
    print(kwargs)
    for key, value in kwargs.items():
        print(f"{key}: {value}")

print_kwargs(name='Alice', age=30, city='New York')
# Output: name: Alice
#         age: 30
#         city: New York
