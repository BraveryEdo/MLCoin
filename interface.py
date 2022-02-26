from tkinter import *
import tkinter
import historical
import mergeData
import sys
import config

window = Tk()
window.title("MLCoin")
window.geometry('350x200')
headerArray ={}
buttonArray ={}

sentiment_data_path = config.sentiment_data_path
csv_path = config.csv_path
MY_CURRENCIES = config.MY_CURRENCIES
START_YEAR_BY_CURRENCY = config.START_YEAR_BY_CURRENCY
time_lens=config.time_lens

def get_multi_res_historical():
    return historical.getMultRes()
    
def process_trades():
    return mergeData.merger()


def show_trade_data():

    display = Toplevel(window)
    display.title("Data Display")
    display.geometry('350x200')
    
    coin_opts = MY_CURRENCIES
    selected_coin = StringVar()
    coin_drop = OptionMenu(display, selected_coin, *coin_opts)
    selected_coin.set(coin_opts[0])
    coin_drop.grid(column=0, row=0)
    
    res_opts = list(time_lens.keys())
    selected_res = StringVar()
    res_drop = OptionMenu(display, selected_res, *res_opts)
    selected_res.set(str(res_opts[0]))
    res_drop.grid(column=1, row=0)
    '''
    years = START_YEAR_BY_CURRENCY
    year_opts = {'ALL':'ALL'}.append(years)
    for sub in years:
        for key in sub:
            sub[key] = str(sub[key])
    selected_year = StringVar()
    year_drop = ttk.ComboBox(display, selected_year, *year_opts)
    selected_year.set(year_opts['ALL'])
    year_drop.grid(column=2, row=0)

  
        

    run=Button(display, text="show" command=)
    run.grid(column=3, row=0)
    '''
    

    

def exit():
    sys.exit()


#organized this way to make addinf functionality super simple
#layout for each new section/button defined programatically and they should all look the same
#header Text for each section
hText=["Aquire Data", "Process Data", "Show Data", ""]
#buttons grouped by section
bText=[["DL Trade Data"], ["Process Trades"], ["Trade Data"], ["Exit"]]
#functions for each button
bFunc=[[get_multi_res_historical],[process_trades],[show_trade_data],[exit]]

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
