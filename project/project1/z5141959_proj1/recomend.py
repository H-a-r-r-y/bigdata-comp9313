import math
from decimal import Decimal

list = [[5,3,4,4],[3,3,1,5]]

av_res = []
for i in list:
    av_res.append([ Decimal(j) - Decimal(sum(i)) / Decimal(len(i)) for j in i ])

print(av_res)

av_res2 = [[float(i) for i in av_res[0]],[float(i) for i in av_res[1]]]

print(av_res2)
# [0.75,-1.25,-0.25,0.75]

list2 = [[2,0,4,0],[0,0,3,2]]

def cos_sim(x,y):
    length = len(x)
    dot_p = 0
    x_len = 0
    y_len = 0
    dot_p_str = ""
    x_len_str = ""
    y_len_str = ""
    for i in range(length):
        dot_p_str += str(x[i]) + "x" + str(y[i]) + " + "
        x_len_str += str(x[i]) + "^2" + "+"
        y_len_str += str(y[i]) + "^2" + "+"

        dot_p += x[i] * y[i]
        x_len += x[i] * x[i]
        y_len += y[i] * y[i]


    b = "sqrt({}) x sqrt({})".format(x_len_str[:-1],y_len_str[:-1])
    print(str(dot_p) + " / "+str(x_len)+" + "+str(y_len)+" = "+ str(dot_p/(math.sqrt(x_len)*math.sqrt(y_len)) ))
    print(dot_p_str[:-2] + " / " +b + " = " + str(dot_p/(math.sqrt(x_len)*math.sqrt(y_len))))


cos_sim(list2[0],list2[1])


