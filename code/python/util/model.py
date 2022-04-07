from pmdarima.arima import auto_arima

def fcast_arima(df_train,x_train=None,x_test=None,n=1,p=2,d=2,q=2,m=12):
    if sum(df_train) == 0:
        return [0]*n
    else:
        try:
            arima = auto_arima(df_train, X=x_train,
                               start_p=0, d=0, start_q=0, max_p=p, max_d=d, max_q=q, start_P=0, D=0, start_Q=0, max_P=p, max_D=d, max_Q=q, m=m,
                               error_action='ignore')
        except:
            return [list(df_train)[-1]]*n
        return arima.predict(n_periods=n, X=x_test)