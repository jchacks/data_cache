from data_cache import Client

c = Client()
a = c.get_or_create_store('a')
b = c.get_or_create_store('b')


# Due to namespaces these values wont overwrite.
a['c'] = 2
b['c'] = 5

assert a['c'] != b['c']

print(a['c'])  # prints 2
print(b['c'])  # prints 5

del a['c']
del b['c']

print(a['c'])  # Throws KeyError
print(b['c'])  # Throws KeyError
