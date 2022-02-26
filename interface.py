from tkinter import *
import tkinter as tk
import historical
import mergeData
import threading

window = Tk()
window.title("MLCoin")
window.geometry('350x200')

headerArray ={}
buttonArray ={}

def get_multi_res_historical():
    return historical.getMultRes()
    
def process_trades():
    return mergeData.merger()

def show_data():
    return True



hText=["Aquire Data", "Process Data", "Show Data"]
bText=[["DL Trade Data"], ["Process Trades"], ["Display Data"]]
bFunc=[[get_multi_res_historical],[process_trades],[show_data]]

def main():
    rPos = 0
    for i in range(len(hText)):
        h = Label(window,text=hText[i])
        h.grid(column=0,row=rPos)
        headerArray[i]= h
        buttonArray[i]={}
        rPos = rPos+1
        for b in range(len(bText[i])):
            b=Button(window, text=bText[i][b], command=bFunc[i][b])
            b.grid(column=1,row=rPos)
            buttonArray[i][b]=b
            rPos = rPos+1
       
    print(headerArray)
    print(buttonArray)


    window.mainloop()
