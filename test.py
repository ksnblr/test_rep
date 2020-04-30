import sys
def about_me(your_name):
    print("The wise {} loves Python.".format(your_name))

def HW(your_name):
    print("Hello {}".format(your_name))

def Both(arg):
    HW(arg)
    about_me(arg)

Both(sys.argv[1])
