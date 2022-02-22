from bs4 import BeautifulSoup
from pprint import pprint 
import numpy as np
import matplotlib.pyplot as plt
import historical
import mergeData
try:
    # [OPTIONAL] Seaborn makes plots nicer
    import seaborn
except ImportError:
    pass

#historical.getMultRes()
#historical.updateNewest()
#mergeData.addReadableTime()
#mergeData.setIndex()
mergeData.merger()

print("done")