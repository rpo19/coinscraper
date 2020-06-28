import sys
from getpass import getpass
if sys.stdin.isatty():
    user = getpass(prompt="User: ")
    passwd = getpass(prompt="Passwd: ")
else:
    user = input()
    passwd = input()

print("usr: {}, pwd: {}".format(user,passwd))
