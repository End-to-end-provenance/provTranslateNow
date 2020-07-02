i = 1
while(i < 6):
	print(i)
	i = i + 1

x = [2,5,3,9,8,11,6]
count = 0
for val in x:
	if (val % 2 == 0):
		count+=1

x = -5
if (x>0):
	x = x - 3
	print("Non-negative number")
else:
	x = x + 3
	print("Negative number")