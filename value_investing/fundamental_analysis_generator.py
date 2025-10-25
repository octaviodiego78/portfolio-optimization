import yfinance as yf
import pandas as pd
import datetime
import os
import warnings
warnings.filterwarnings('ignore')


class InvestingAnalysis:
    def __init__(self):
        pass

    def get_historical_price(self, ticker: str, start: str):
        end = start+datetime.timedelta(days=4)
        price=pd.DataFrame(yf.Ticker(ticker).history(start=start, end=end)["Close"])
        return price.iloc[0, 0]

    def fundamental_analysis(self, ticker: str, use_quarterly: bool = False):
        info = yf.Ticker(ticker)
        if info is None:
            print(f"   - Error: Ticker {ticker} not found")
            return None

        if use_quarterly:
            bg = info.quarterly_balance_sheet.iloc[:,:-1]
            edo = info.quarterly_income_stmt.iloc[:,:-1]
            cashflow = info.quarterly_cashflow
        else:
            bg = info.balance_sheet.iloc[:,:-1]
            edo = info.incomestmt.iloc[:,:-1]
            cashflow = info.cashflow

        if bg.size == 0 or edo.size == 0 or cashflow.size == 0:
            print(f"   - Error: Incomplete data for {ticker}. Won't be analyzed")
            return None
    
        incomplete = False

        try:
            income = edo.loc['Net Income Common Stockholders']
            n_shares = bg.loc['Ordinary Shares Number']

            # Calculate Earning Per Share (EPS)
            eps = income / n_shares
            eps.sort_index(inplace=True, ascending=False)

        except Exception as e:
            print(f"   - Error in EPS calculation: {e}")
            eps = None
            incomplete = True

        try:
            shares = bg.loc['Ordinary Shares Number']

        except Exception as e:
            print(f"   - Error in Shares calculation: {e}")
            shares = None
            incomplete = True
            

        try:
            prices = pd.Series([self.get_historical_price(ticker, date) for date in eps.index], index=eps.index)
            prices.sort_index(inplace=True, ascending=False)


            #Price Earnings Ratio (PER)
            per = prices / eps
            per.sort_index(inplace=True, ascending=False)

        except Exception as e:
            print(f"   - Error in PER calculation: {e}")
            per = None
            incomplete = True

        try:
            #EBITDA
            ebitda = edo.loc['EBITDA']
            ebitda.sort_index(inplace=True, ascending=False)

        except Exception as e:
            print(f"   - Error in EBITDA calculation: {e} ")
            ebitda = None
            incomplete = True
        try:
            #Price Book Value (PBV)
            bvps = bg.loc[ 'Common Stock Equity'] / n_shares
            pbv = prices / bvps
            pbv.sort_index(inplace=True, ascending=False)
        except Exception as e:
            print(f"   - Error in PBV calculation: {e}")
            pbv = None
            incomplete = True
        
        try:
            #Solvency
            assets = bg.loc['Total Assets']
            liabilities = bg.loc['Current Liabilities']
            solvency = assets / liabilities
            solvency.sort_index(inplace=True, ascending=False)

        except Exception as e:
            print(f"   - Error in Solvency calculation: {e}")
            solvency = None
            incomplete = True

        try:
            #Return on Shareholder Equity (ROE)
            equity = bg.loc[ 'Common Stock Equity']
            roe = (income / equity) * 100
            roe.sort_index(inplace=True, ascending=False)

        except Exception as e:  
            print(f"   - Error in ROE calculation")
            roe = None
            incomplete = True
        try:
            #Free Cashflow
            cfo = cashflow.loc['Operating Cash Flow']
            non_cash_charges = cashflow.loc['Depreciation And Amortization']
            inc_working_capital = bg.loc['Total Assets'] - bg.loc['Current Liabilities']
            fcf = cfo + non_cash_charges - inc_working_capital
            fcf.sort_index(inplace=True, ascending=False)
            
        except Exception as e   :
            print(f"   - Error in FCF calculation: {e}")
            fcf = None
            incomplete = True

        return {
            'eps': eps,
            'per': per,
            'ebitda': ebitda,
            'pbv': pbv,
            'solvency': solvency,
            'roe': roe,
            'fcf': fcf,
            'prices': prices,
            'shares': shares,
            'incomplete': incomplete
        }

    def dataset_generator(self, tickers: list[str]):
        """
        Generates a dataset of fundamental analysis for a list of tickers.
        """
        results = pd.DataFrame()
        results_path = os.path.join(os.getcwd(), 'value_investing', 'results.csv')
        results.to_csv(results_path, index=False)

        incomplete = pd.DataFrame()
        incomplete_path = os.path.join(os.getcwd(), 'value_investing', 'incomplete.csv')
        incomplete.to_csv(incomplete_path, index=False)

        # Batch accumulator
        batch = pd.DataFrame()
        incomplete_list = []
        for i, ticker in enumerate(tickers):
            try:
                print(f"Processing ticker {ticker}:  {i}/{len(tickers)}")
                result = self.fundamental_analysis(ticker, use_quarterly=False)
                result = pd.DataFrame(result)
                result['ticker'] = ticker
                result['returns'] = result['prices'].pct_change()
                
                result = result.dropna(how='all', subset=[col for col in result.columns if col not in ['ticker', 'incomplete']])
                result['date'] = result.index
                
                if result['incomplete'].sum() > 0:
                    incomplete_list.append(ticker)
            
                results = pd.concat([results, result])

                # Add to batch
                batch = pd.concat([batch, result])
                results = pd.concat([results, result])
                
                # Save every 20 iterations OR on the last iteration
                if (i + 1) % 20 == 0 or (i + 1) == len(tickers):
                    cwd = os.getcwd()
                    path = os.path.join(cwd, 'value_investing', 'results.csv')
                    batch.to_csv(path, mode='a', index=False)
                    batch = pd.DataFrame()

                    if incomplete_list:
                        path = os.path.join(cwd, 'value_investing', 'incomplete.csv')
                        pd.DataFrame({'ticker': incomplete_list}).to_csv(path, index=False, mode='a')
                        incomplete_list = []
        
            except Exception as e:
                print(f"ERROR in dataset generator main: {ticker}")
                print(f"   - Error: {e}")
                continue
            
                    

        return results
        
if __name__ == "__main__":
    investing_analysis = InvestingAnalysis()

    #Getting all SP500 tickers
    cwd = os.getcwd()
    path = os.path.join(cwd, 'value_investing', 'sp500_companies.csv')
    sp500 = pd.read_csv(path) 
    sp_500_tickers = sp500['Symbol'].tolist()

    #Generating the dataset
    final = investing_analysis.dataset_generator(sp_500_tickers)
    


