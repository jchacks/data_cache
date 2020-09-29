from data_cache.data_cache import Client

c = Client('generic')
c.connect()

d = Client('data')
d.connect()


# Due to namespaces these values wont overwrite.
c['a'] = 2
d['a'] = 5

print(c['a'])  # prints 2
print(d['a'])  # prints 5

del c['a']
del d['a']

print(c['a'])  # Throws KeyError
print(d['a'])  # Throws KeyError
