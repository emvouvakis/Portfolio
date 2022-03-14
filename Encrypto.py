import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from tkinter import *

root = Tk()
root.title("Encrypto")
root.iconbitmap('C:/Users/manol/OneDrive/Έγγραφα/Python/icon.ico')



canvas=Canvas(root, width=450, height=600)
canvas.grid(columnspan=2, rowspan=3)



def get_data():
    global salt,pwd,f_add,e3,e4
    pwd=e1.get().encode()
    salt=e2.get().encode()
    f_add=LabelFrame(root, text="Usage - Password", padx=5, pady=5)
    f_add.grid(column=0,row=1)
    e3=Entry(f_add,width=20)
    e3.grid(column=0, row=1)
    e4=Entry(f_add,width=20)
    e4.grid(column=1, row=1)
    button_add1= Button(f_add, text="Add Password", command=add)
    button_add1.grid(column=2,row=1)
    view()

def add():
    global salt
    kdf = PBKDF2HMAC(
    algorithm=hashes.SHA256(),
    length=32,
    salt=salt,
    iterations=390000)

    key = base64.urlsafe_b64encode(kdf.derive(pwd))
    f = Fernet(key)
    name=e3.get()
    given_pwd=e4.get()
    with open('passwords.txt', 'a') as p:
        p.write(name + "|" + f.encrypt(given_pwd.encode()).decode() + "\n")
    view()

def view():
    global salt
    counter=0
    kdf = PBKDF2HMAC(
    algorithm=hashes.SHA256(),
    length=32,
    salt=salt,
    iterations=390000)
    key = base64.urlsafe_b64encode(kdf.derive(pwd))
    f = Fernet(key)
    f_box=LabelFrame(root, text="View Passwords", padx=5, pady=5)
    f_box.grid(column=0,row=2)
    #button_view= Button(f_box, text="View", command=view)
    #button_view.grid(column=2, row=2)
    with open('passwords.txt', 'r') as p:
        box=Text(f_box, height=15, width=45)
        box.grid(column=0,row=2)
        for line in p.readlines():
            counter+=1
            if counter==0:
                break
            data = line.rstrip()
            user, v_pwd = data.split("|")
            try: 
                decr_pwd= f.decrypt(v_pwd.encode()).decode()
                result=str("Usage:"+ user+ " Password: "+ decr_pwd+ "\n")
                box.insert(END, result)
            except:
                result="Wrong Keys \n"
                box.insert(END, result)


'''First stage-Buttons and Entries to start'''
f_keys=LabelFrame(root, text="Key 1 - Key 2", padx=5, pady=5)
f_keys.grid(column=0,row=0)

button_keys= Button(f_keys, text="Use Keys",command=get_data)
button_keys.grid(column=2,row=0)


e1=Entry(f_keys,width=20)
e1.grid(column=0, row=0)
e2=Entry(f_keys,width=20)
e2.grid(column=1, row=0)

'''Information'''
f_info=LabelFrame(root, text="Information", padx=5, pady=5)
f_info.grid(column=0,row=2)
b_info=Text(f_info, height=15, width=45)
b_info.grid(column=0,row=2)
hyperlink='https://emvouvakis.github.io/'
result="Welcome to Encrypto! \n\nProject for password encryption. \nMade by : Emmanouil Vouvakis. \n\n\nYou can learn more about me in:\n"+hyperlink

b_info.insert(END, result)



root.mainloop()