import pandas as pd
from matplotlib import pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.arima.model import ARIMA
from math import sqrt
from sklearn.metrics import mean_squared_error

series=pd.read_csv('/home/petros/Downloads/File_series.csv', header=0, parse_dates=[0], index_col=0)

print(series.index)
print(series)

result = seasonal_decompose(series['values'], model='additive', period=365)

result.plot()
plt.show()

X = series['values'].values
size = int(len(X) * 0.76)
train, test = X[0:size], X[size:len(X)]
history = [x for x in train]
predictions = list()

# Train and forecast using ARIMA model
for t in range(len(test)):
	
    model = ARIMA(history, order=(5, 1, 0))
    model_fit = model.fit()
    output = model_fit.forecast()
    yhat = output[0]
    predictions.append(yhat)
    obs = test[t]
    history.append(obs)
    print('predicted=%f, expected=%f' % (yhat, obs))

rmse = sqrt(mean_squared_error(test, predictions))
print('Test RMSE: %.3f' % rmse)

plt.plot(test)
plt.plot(predictions, color='red')
plt.xlabel('Time')
plt.ylabel('Least Price')
plt.title('Actual vs Predicted Least Prices')
plt.legend(['Actual', 'Predicted'])
plt.show()
