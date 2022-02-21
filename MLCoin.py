from bs4 import BeautifulSoup
from pprint import pprint 
import numpy as np
import matplotlib.pyplot as plt
import historical
try:
    # [OPTIONAL] Seaborn makes plots nicer
    import seaborn
except ImportError:
    pass

historical.getHistorical()

#historical.other()

print("done")