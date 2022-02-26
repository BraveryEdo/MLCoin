'''
from bs4 import BeautifulSoup
from pprint import pprint 
import numpy as np
import matplotlib.pyplot as plt
try:
    # [OPTIONAL] Seaborn makes plots nicer
    import seaborn
except ImportError:
    pass
'''
import historical
import mergeData
import interface

interface.main()

# Code to add widgets will go here...
'''
historical.getMultRes()
mergeData.merger()
'''
print("done")