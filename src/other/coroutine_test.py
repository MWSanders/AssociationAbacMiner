# def coroutine(func):
#     def start(*args, **kwargs):
#         cr = func(*args, **kwargs)
#         next(cr)
#         return cr
#     return start
#
# @coroutine
# def produce(key, target):
#     while True:
#         n = (yield)
#         target.send((key, n*10))
#
# class Sink(object):
#
#     def __init__(self):
#         self.d = {}
#         self.co = self.sink()
#
#     def send(self, *args):
#         self.co.send(*args)
#
#     @coroutine
#     def sink(self):
#         try:
#             while True:
#                 key, n = yield
#                 if key in self.d:
#                     self.d[key] = max(self.d[key], n)
#                 else:
#                     self.d[key] = n
#         except GeneratorExit:
#             pass
#
#
# sk = Sink()
# pipeA = produce("A", sk)
# pipeB = produce("B", sk)
#
# pipeA.send(10)
# pipeA.send(20)
# pipeA.send(40)
#
# pipeB.send(20)
# pipeB.send(40)
# pipeB.send(60)
#
# print(sk.d.items())


# Python3 program for demonstrating
# coroutine execution

def print_name(prefix):
    print("Searching prefix:{}".format(prefix))
    while True:
        name = (yield)
        if prefix in name:
            print(name)


# calling coroutine, nothing will happen
corou = print_name("Dear")

# This will start execution of coroutine and
# Prints first line "Searchig prefix..."
# and advance execution to the first yield expression
corou.__next__()

# sending inputs
corou.send("Atul")
corou.send("Dear Atul")


inputs = []
def store_inputs():
    while True:
        input = yield
        inputs.append(input)
        print(input)

si = store_inputs()
si.__next__()
for i in range(0,10):
    si.send(i)
si.close()
print(inputs)