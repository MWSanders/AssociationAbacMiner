import Orange
from orangecontrib.associate.fpgrowth import *
import operator

data = Orange.data.Table('zoo')
print(data)
X, mapping = OneHot.encode(data, include_class=False)
names = {item: '{}={}'.format(var.name, val)
         for item, var, val in OneHot.decode(mapping, data, mapping)}

# print(X)
# print(sorted(mapping.items()))
# print(names)

itemsets = dict(frequent_itemsets(X, .5))
#sorted_itemset = sorted(itemsets.items(), key=operator.itemgetter(1), reverse=True)

def calculate_over_assign(item, X, mapping, data):
    yes=0
    no=0
    indexes = set([index for index in item])
    column_value_map = {}

    for index in indexes:
        column, value = mapping[index]
        column_value_map[column] = value

    covered_row_columns = 0
    uncovered_row_columns = 0
    uncovered_rows = 0
    for x in range(0, len(data.X)):
        row = data.X[x]
        has_all_constraints = True
        for column, value in column_value_map.items():
            if row[column] != value:
                has_all_constraints = False
        row_all_columns_covered = True
        for y in range(0, len(row)):
            value = row[y]
            if value > 0:
                if y not in column_value_map:
                    uncovered_row_columns += 1
                    row_all_columns_covered = False
                elif y in column_value_map and column_value_map[y] != value:
                    uncovered_row_columns += 1
                    row_all_columns_covered = False
                else:
                    covered_row_columns += 1
            elif value == 0.0:
                if y in column_value_map and column_value_map[y] != value:
                    uncovered_row_columns += 1
                    row_all_columns_covered = False
                else:
                    covered_row_columns += 1
        if not row_all_columns_covered:
            uncovered_rows += 1
    columns_covered = covered_row_columns / (covered_row_columns + uncovered_row_columns)
    uncovered_columns = uncovered_row_columns / (covered_row_columns + uncovered_row_columns)
    #overassignment = uncovered_rows / len(data.X)
    return uncovered_columns

itemsets_by_overassigment = []
for item, count in itemsets.items():
    coverage = count / data.n_rows
    overassignment = calculate_over_assign(item, X, mapping, data)
    print('%s ---> %f, %f' % (', '.join(names[i] for i in item), coverage, overassignment))
    itemsets_by_overassigment.append((overassignment, coverage, ', '.join(names[i] for i in item)))

sorted_by_overassignment = sorted(itemsets_by_overassigment, key=operator.itemgetter(0), reverse=False)
# print(sorted_by_overassignment)
