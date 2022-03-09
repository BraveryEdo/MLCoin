import os
from tkinter import XView
import pandas
import numpy as np
import matplotlib.pyplot as plt
import config
import sklearn

try:
    # [OPTIONAL] Seaborn makes plots nicer
    import seaborn
except ImportError:
    pass

# =====================================================================

def get_features_and_labels(frame):
    '''
    Transforms and scales the input data and returns numpy arrays for
    training and testing inputs and targets.
    '''

    # Replace missing values with 0.0
    # or we can use scikit-learn to calculate missing values below
    #frame[frame.isnull()] = 0.0
    # Convert values to floats
    arr = np.array(frame, dtype=np.float)

    # Normalize the entire data set
    from sklearn.preprocessing import StandardScaler, MinMaxScaler
    arr = MinMaxScaler().fit_transform(arr)

    # Use the last column as the target value
    X, y = arr[:, 1:], arr[:, 0]

    #print(f'training sets:\nX: {X[0]}, \ny:{y[0]}')

    # To use the first column instead, change the index value
    #X, y = arr[:, 1:], arr[:, 0]
    
    # Use 50% of the data for training, but we will test against the
    # entire set
    from sklearn.model_selection import train_test_split
    X_train, _, y_train, _ = train_test_split(X, y, test_size=0.5)
    X_test, y_test = X, y
    
    # If values are missing we could impute them from the training data
    #from sklearn.preprocessing import Imputer
    #imputer = Imputer(strategy='mean')
    #imputer.fit(X_train)
    #X_train = imputer.transform(X_train)
    #X_test = imputer.transform(X_test)
    
    # Normalize the attribute values to mean=0 and variance=1
    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    # To scale to a specified range, use MinMaxScaler
    #from sklearn.preprocessing import MinMaxScaler
    #scaler = MinMaxScaler(feature_range=(0, 1))
    
    # Fit the scaler based on the training data, then apply the same
    # scaling to both training and test sets.
    scaler.fit(X_train)
    X_train = scaler.transform(X_train)
    X_test = scaler.transform(X_test)

    # Return the training and test sets
    return X_train, X_test, y_train, y_test


# =====================================================================


def evaluate_learner(X_train, X_test, y_train, y_test):
    '''
    Run multiple times with different algorithms to get an idea of the
    relative performance of each configuration.

    Returns a sequence of tuples containing:
        (title, expected values, actual values)
    for each learner.
    '''

    # Use a support vector machine for regression
    from sklearn.svm import SVR

    # Train using a radial basis function
    print('radial basis function training has begun')
    svr = SVR(kernel='rbf', gamma=0.1)
    svr.fit(X_train, y_train)
    y_pred = svr.predict(X_test)
    r_2 = svr.score(X_test, y_test)
    yield 'RBF Model ($R^2={:.3f}$)'.format(r_2), y_test, y_pred

    # Train using a linear kernel
    print('linear kernel training has begun')
    svr = SVR(kernel='linear')
    svr.fit(X_train, y_train)
    y_pred = svr.predict(X_test)
    r_2 = svr.score(X_test, y_test)
    yield 'Linear Model ($R^2={:.3f}$)'.format(r_2), y_test, y_pred
    '''
    # Train using a polynomial kernel
    print('polynomial kernel training has begun')
    svr = SVR(kernel="poly", gamma="auto", degree=3, epsilon=0.1, coef0=1)
    svr.fit(X_train, y_train)
    y_pred = svr.predict(X_test)
    r_2 = svr.score(X_test, y_test)
    yield 'Polynomial Model ($R^2={:.3f}$)'.format(r_2), y_test, y_pred
    '''

# =====================================================================


def plot(results, UF_time):
    '''
    Create a plot comparing multiple learners.

    `results` is a list of tuples containing:
        (title, expected values, actual values)
    
    All the elements in results will be plotted.
    '''

    # Using subplots to display the results on the same X axis
    fig, plts = plt.subplots(nrows=len(results), figsize=(12, 8))
    
    
    #plt.xticks(range(len(UF_time)), UF_time, rotation = 30, fontsize = 'xx-small')
    fig.canvas.set_window_title('Prediction data ...')

    # Show each element in the plots returned from plt.subplots()
    for subplot, (title, y, y_pred) in zip(plts, results):
        # Configure each subplot to have no tick marks
        # (these are meaningless for the sample dataset)
        # Label the vertical axis
        subplot.set_ylabel('stock price')

        
        # Set the title for the subplot
        subplot.set_title(title)

        # Plot the actual data and the prediction
        subplot.plot(y, 'b', label='actual')
        subplot.plot(y_pred, 'r', label='predicted')
        
        # Shade the area between the predicted and the actual values
        subplot.fill_between(
            # Generate X values [0, 1, 2, ..., len(y)-2, len(y)-1]
            np.arange(0, len(y), 1),
            y,
            y_pred,
            color='r',
            alpha=0.2
        )

        # Mark the extent of the training data
        subplot.axvline(len(y) // 2, linestyle='--', color='0', alpha=0.2)

        # Include a legend in each subplot
        subplot.legend()

    # Let matplotlib handle the subplot layout
    fig.tight_layout()

    # ==================================
    # Display the plot in interactive UI
    plt.show()


    # To save the plot to an image file, use savefig()
    #plt.savefig('plot.png')

    # Open the image file with the default image viewer
    #import subprocess
    #subprocess.Popen('plot.png', shell=True)

    # To save the plot to an image in memory, use BytesIO and savefig()
    # This can then be written to any stream-like object, such as a
    # file or HTTP response.
    #from io import BytesIO
    #img_stream = BytesIO()
    #plt.savefig(img_stream, fmt='png')
    #img_bytes = img_stream.getvalue()
    #print('Image is {} bytes - {!r}'.format(len(img_bytes), img_bytes[:8] + b'...'))

    # Closing the figure allows matplotlib to release the memory used.
    plt.close()


    '''
    csv data structure:
    root:.\csv_data
    root level: csv_data\\{RESOLUTION:1min}_res:
        currency folder {CURRENCY:BTC-USD}:
            full summry file *if generated*: FULL_{BTC-USD}_{RESOLUTION}.csv
            year folder {YEAR:2020}:
                data: {BTC-USD}_{YEAR}_{sequence}.csv
                yearly data summary file *if generated*: {YEAR}_{BTC-USD}_{RESOLUTION}.csv
    '''
def run(c_var, r_var, y_var):
    csv_path = config.csv_path
    c = c_var.get()
    r = r_var.get()
    y = y_var
    path = ''
    if y == 0:
        path = csv_path+ '\\'+r+'_res\\'+c+'\\FULL_'+c+'_'+r+'.csv'
    elif os.path.isdir(csv_path+'\\'+r+'_res\\'+c+'\\'+y):

        #check to see if year is available in data
        path = csv_path+ f'\\{r}_res\\{c}\\{y}\\{y}_{c}_{r}.csv'
    else:
        print("year selection is causing problems")
        
    if os.path.exists(path):
        frame = pandas.read_csv(path, usecols=['time','high', 'low', 'volume', 'UF_time'])
        UFT = frame['UF_time']
        frame['year'] = pandas.to_datetime(frame['time']).dt.year
        frame['month'] = pandas.to_datetime(frame['time']).dt.month
        frame['day'] = pandas.to_datetime(frame['time']).dt.day
        frame['weekday'] = pandas.to_datetime(frame['time']).dt.weekday
        frame['hour'] = pandas.to_datetime(frame['time']).dt.hour
        frame['minute'] = pandas.to_datetime(frame['time']).dt.minute
        #put high (prediction target) in the first spot
        frame = frame[['high', 'time', 'volume', 'low', 'year', 'month', 'day', 'weekday', 'hour', 'minute']]
        #frame = pandas.read_csv(path, usecols=['time','low','high', 'open', 'close', 'volume'])
        # Process data into feature and label arrays
        print("Processing {} samples with {} attributes".format(len(frame.index), len(frame.columns)))
        X_train, X_test, y_train, y_test = get_features_and_labels(frame)

        # Evaluate multiple regression learners on the data
        print("Evaluating regression learners")
        results = list(evaluate_learner(X_train, X_test, y_train, y_test))

        # Display the results
        print("Plotting the results")
        plot(results, UFT)
    else:
        print(f'cant read csv from {path}')

    

