import pandas as pd
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

import warnings
warnings.filterwarnings("ignore")

def merge_dataframes(file_paths):
    """
    Merges multiple data files based on the 'date' column and returns the merged dataframe.

    Parameters:
    file_paths (list of str): List of file paths to the CSV files to be merged.
    """
    dfs = []


    # Getting values from all files and changing column name 'value' to each specific filename_value
    for file_path in file_paths:
        df = pd.read_csv(file_path)
        df['date'] = pd.to_datetime(df['date'])
        value_column_name = file_path.split('\\')[-1].replace('.csv', '') + '_value'
        df.rename(columns={'value': value_column_name}, inplace=True)
        dfs.append(df)

    # Fetching common dates in all features
    common_dates = dfs[0]['date']
    for df in dfs[1:]:
        common_dates = common_dates[common_dates.isin(df['date'])]

    #Keeping only those dates that are common
    dfs = [df[df['date'].isin(common_dates)] for df in dfs]

    #Making a dataframe with 1st column as date and other features values 
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = merged_df.merge(df, on='date')

    return merged_df

def read_data(merged_df):
    """
    Prepare data for modeling by dropping missing values and the 'date' column.

    Parameters:
    merged_df (DataFrame): Merged dataframe from merge_dataframes function.

    Returns:
    DataFrame: Processed data ready for modeling.
    """
    data = merged_df.dropna()
    data = data.drop('date', axis=1)
    return data

def linear_regression_equation(data):
    """
    Fit a linear regression model and return the fitted model along with the equation.

    Parameters:
    data (DataFrame): Input data with features and target variable.

    Returns:
    model: Fitted linear regression model.
    equation (str): Equation of the linear regression model.
    """

    X = data.iloc[:, :-1]  
    y = data.iloc[:, -1]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    scaler_X = StandardScaler()
    X_train_scaled = scaler_X.fit_transform(X_train)

    scaler_y = StandardScaler()
    y_train_scaled = scaler_y.fit_transform(y_train.values.reshape(-1, 1))

    model = LinearRegression()
    model.fit(X_train_scaled, y_train_scaled.ravel())

    # Equation coefficients
    coef = model.coef_
    intercept = model.intercept_

    equation = {col: coef[i] for i, col in enumerate(X_train.columns)}
    equation['constant'] = intercept

    return model,equation

def polynomial_regression_equation(data):
    """
    Fit a polynomial regression model with hyperparameter tuning and scaling, and return the fitted model along with the equation.

    Parameters:
    data (DataFrame): Input data with features and target variable.

    Returns:
    model: Fitted polynomial regression model.
    equation (str): Equation of the polynomial regression model.
    X_train_scaled (numpy.ndarray): Scaled training features.
    y_train_scaled (numpy.ndarray): Scaled training target variable.
    """
    param_grid = {
        'poly__degree': [2, 3, 4, 5]
    }

    X = data.iloc[:, :-1]  
    y = data.iloc[:, -1]

    # Splitting the data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    pipeline = Pipeline([
        ('poly', PolynomialFeatures()), 
        ('scaler', StandardScaler()), 
        ('regressor', LinearRegression())
    ])

    grid_search = GridSearchCV(pipeline, param_grid, cv=5)
    grid_search.fit(X_train, y_train)

    best_degree = grid_search.best_params_['poly__degree']
    best_model = grid_search.best_estimator_

    coef = best_model.named_steps['regressor'].coef_
    intercept = best_model.named_steps['regressor'].intercept_

    equation = {f"{col}^{i}": coef[j] for i in range(1, best_degree+1) for j, col in enumerate(X.columns)}
    equation['constant'] = intercept

    return best_model, equation

def inference(X,y):
    rf_model = RandomForestRegressor(random_state = 42)
    rf_model.fit(X, y)
    y_pred = rf_model.predict(X)
    return y_pred

def linear_model_inference(model, X, y):
    """
    Perform inference using the linear regression model.

    Parameters:
    model: Fitted linear regression model.
    X (DataFrame): Features.
    y (DataFrame): Target variable.

    Returns:
    rmse (float): Root Mean Squared Error.
    inference (numpy.ndarray): Model predictions.
    """
    y_pred = model.predict(X)
    rmse = mean_squared_error(y, y_pred,squared=False)
    return rmse, y_pred

def polynomial_model_inference(model, X, y):
    """
    Perform inference using the polynomial regression model.

    Parameters:
    model: Fitted polynomial regression model.
    X_test (DataFrame): Test features.
    y_test (DataFrame): Test target variable.

    Returns:
    rmse (float): Root Mean Squared Error.
    inference (numpy.ndarray): Model predictions.
    """
    y_pred = model.predict(X)
    rmse = mean_squared_error(y, y_pred,squared=False)
    return rmse, y_pred


file_paths = [
    r'C:\Users\anubhav\Desktop\leverslabs\data_model\accept_opps_weekly_latest.csv',
    r'C:\Users\anubhav\Desktop\leverslabs\data_model\new_prosps_weekly_latest.csv',
    r'C:\Users\anubhav\Desktop\leverslabs\data_model\new_sales_accept_leads_weekly_latest.csv',
    r'C:\Users\anubhav\Desktop\leverslabs\data_model\sqls_weekly_latest.csv',
    r'C:\Users\anubhav\Desktop\leverslabs\data_model\new_biz_deals_weekly_latest.csv'
]

def modelling():
    merge_df = merge_dataframes(file_paths) 
    data = read_data(merge_df)

    X = data.iloc[:, :-1]  
    y = data.iloc[:, -1]

    # Linear regression model and equation
    linear_model, linear_equation = linear_regression_equation(data)

    # Polynomial regression model and equation
    polynomial_model, polynomial_equation = polynomial_regression_equation(data)

    linear_rmse,linear_pred = linear_model_inference(linear_model,X,y) # linear model inference
    
    poly_rmse, poly_pred = polynomial_model_inference(polynomial_model,X,y) # polynomial model inference

    data_inference = inference(X,y) # Inference over any data using Random Forest
    
    # return model, equation,linear_inference, poly_inference and data_inference
    if linear_rmse<=poly_rmse:
        return linear_model, linear_equation,linear_pred,poly_pred,data_inference
    else:
        return polynomial_model,polynomial_equation,linear_pred,poly_pred,data_inference

if __name__ == "__main__":
    model,equation,linear_inference,poly_inference,data_inference = modelling()


"""
model: It is the final model returned whether linear or polynomial based on rmse value
equation: It is the equation of returned model whose rmse value is less
linear_inference: This is the inference for linear model 
poly_inference: This is the inference for polynomial model
data_inference: This is the inference using random forest model 
"""