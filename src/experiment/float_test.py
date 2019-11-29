
covR = 0.24577818347786398
overR = 1-9.990976150341012e-08
beta = 100000

for beta in [100000, 1, 1/100000]:
    product = (beta * covR) * overR
    print(product)