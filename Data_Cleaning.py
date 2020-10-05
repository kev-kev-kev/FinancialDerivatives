#### ---------------- ####
####  Gathering Data  ####
#### ---------------- ####

# Creating Objects for spx
spx = yf.Ticker("^SPX")
spx_snapshot = spx.history(period="5m", interval = "1m")

# We'll be using this for a GARCH-type of Model
spx_hist = yf.Ticker("^GSPC").history(period='max', interval='1wk')
spx_close = pd.DataFrame(spx_hist["Close"])

spx_calls = {}
spx_puts = {}
for expiration in spx.options:
    # Defining the name of the current setup
    key = jsDumps(expiration)
    key = key.replace("\"", "")

    # Getting the current options Object
    temp_options_chain = spx.option_chain(expiration)
    print('Gathering options Data for: ', key)
    
    # Saving only the call values
    spx_calls[key] = temp_options_chain.calls
    spx_puts[key] = temp_options_chain.puts

# Yield Data. Sourced from https://bit.ly/2S79FX9
treasury_yield = {'1Mo': [0.09],
                  '2Mo': [0.10],
                  '3Mo': [0.11],
                  '6Mo': [0.11],
                  '1Yr': [0.12],
                  '2Yr': [0.14],
                  '3Yr': [0.16],
                  '5Yr': [0.26]}

pd.DataFrame(treasury_yield, index = ["2020-09-29"] )


# Save data for later use
with open('data/spx_calls.pickle', 'wb') as fp:
    pickle.dump(spx_calls, fp, protocol=pickle.HIGHEST_PROTOCOL)
with open('data/spx_puts.pickle', 'wb') as fp:
    pickle.dump(spx_puts, fp, protocol=pickle.HIGHEST_PROTOCOL)
with open('data/spx_snapshot.pickle', 'wb') as fp:
    pickle.dump(spx_snapshot, fp, protocol=pickle.HIGHEST_PROTOCOL)
with open('data/spx_hist.pickle', 'wb') as fp:
    pickle.dump(spx_hist, fp, protocol=pickle.HIGHEST_PROTOCOL)
with open('data/spx_hist_close.pickle', 'wb') as fp:
    pickle.dump(spx_close, fp, protocol=pickle.HIGHEST_PROTOCOL)
with open('data/treasury_yield.pickle', 'wb') as fp:
    pickle.dump(treasury_yield, fp, protocol=pickle.HIGHEST_PROTOCOL)

#### ---------------- ####
####  Cleaning Data   ####
#### ---------------- ####

# When this script was run, the date was 30-Oct-2020
SPOT_PRICE = spx_snapshot.iloc[-1,1]
SPOT_DATE = datetime(2020, 9, 30)

def return_yield(tt_expiration):
    """
    This function returns the correct yield for an investment. Based on US
    treasury yields. 

    Input
    -----
    tt_expiration: Time to expiration in years
    """
    return 0
    # # Select correct yield
    # if (tt_expiration < 1.5/12):
    #     rate = treasury_yield['1Mo'][0]
    # elif (tt_expiration < 2.5/12):
    #     rate = treasury_yield['2Mo'][0]
    # elif (tt_expiration < 4.5/12):
    #     rate = treasury_yield['3Mo'][0]
    # elif (tt_expiration < 9/12):
    #     rate = treasury_yield['6Mo'][0]
    # elif (tt_expiration < 1.5):
    #     rate = treasury_yield['1Yr'][0]
    # elif (tt_expiration < 2.5):
    #     rate = treasury_yield['2Yr'][0]
    # elif (tt_expiration < 4):
    #     rate = treasury_yield['3Yr'][0]
    # elif (tt_expiration < 6):
    #     rate = treasury_yield['5Yr'][0]
    # else:
    #     print("No rate is selected! Pbbly the tt_expiration is wrong.")
    # return rate

# Create dataframe for calls and puts
calls_df = []
puts_df = []

# We loop over each expiration date (different options chains).
maturities = spx_calls.keys()
for expiration in maturities:
    # Calculate time to expiration
    date = datetime.strptime(expiration, '%Y-%m-%d')
    tt_expiration = (date - SPOT_DATE).days # in days
    tt_expiration = tt_expiration/365 # annualize

    # We do not want short expiration options. They behave erratically
    if tt_expiration < 1/12:
        continue 

    rate = return_yield(tt_expiration)

    calls_chain = spx_calls[expiration]
    calls_chain['premium'] = (calls_chain['ask'] + calls_chain['bid'])/2
    for index, row in calls_chain.iterrows():
        # Conditions for Data Cleaning:
        # (1) Enough liquidity as measured by OI and premium costs. 
        # (2) Strikes not too far in the tails. This is related to the
        # liquidity condition. Strikes at the tails have small interest for
        # investors, and therefore the prices on these contracts do not
        # behave "correctly".
        # (3) Options needs to be worth more than their intrinsic value 
        # (true only for calls). 
        # (3) The last IV check verifies some of the previous conditions.

        strike = row['strike']
        premium = row['premium']
        open_interest = row['openInterest']
        yahoo_iv = row['impliedVolatility']

        bool_higher_intrinsic = (premium - max(SPOT_PRICE-strike, 0)) > 0
        bool_iv_check = yahoo_iv > 0.05
        bool_enough_liquidity = open_interest > 200 
        bool_not_in_tails = strike > 2390 and strike < 4300
        
        if (bool_enough_liquidity and bool_not_in_tails and bool_iv_check 
            and bool_higher_intrinsic):
            calls_df.append([strike, tt_expiration, premium, yahoo_iv, rate])

    puts_chain = spx_puts[expiration]
    puts_chain['premium'] = (puts_chain['ask'] + puts_chain['bid'])/2
    for index, row in puts_chain.iterrows():
        strike = row['strike']
        premium = row['premium']
        open_interest = row['openInterest']
        yahoo_iv = row['impliedVolatility']

        bool_higher_intrinsic = (premium - max(SPOT_PRICE-strike, 0)) > 0
        bool_iv_check = yahoo_iv > 0.05
        bool_enough_liquidity =  open_interest > 200
        bool_not_in_tails = strike > 2390 and strike < 4300

        if (bool_enough_liquidity and bool_not_in_tails and bool_iv_check):
            puts_df.append([strike, tt_expiration, premium, yahoo_iv, rate])

calls_df = pd.DataFrame(calls_df, columns=['Strike', 
                                           'Expiration (Years)', 
                                           'Premium', 
                                           'yahoo_IV', 
                                           'Rate'])
puts_df = pd.DataFrame(puts_df, columns=['Strike', 
                                           'Expiration (Years)', 
                                           'Premium', 
                                           'yahoo_IV', 
                                           'Rate'])
