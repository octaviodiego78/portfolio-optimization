import pandas as pd
import numpy as np
from pyxirr import xirr
import warnings
warnings.filterwarnings('ignore')

# contratos
CONTRATOS = ["20486403", "12861603", "AHA84901"]


def clean_balance_data(df_bal):
    """
    Clean balance data by filtering and aggregating by contract and date.
    Returns a clean dataframe with contract, date, and total portfolio value.
    """
    df_bal = df_bal.drop_duplicates()
    df_bal = df_bal[["contract", "balance_date", "value_pos_mdo"]]
    df_bal["value_pos_mdo"] = pd.to_numeric(df_bal["value_pos_mdo"], errors="coerce")
    df_bal = df_bal[df_bal["contract"].astype(str).isin(CONTRATOS)]

    df_port = (df_bal
               .groupby(["contract", "balance_date"], as_index=False, dropna=True)["value_pos_mdo"]
               .sum()
               .rename(columns={
                   "contract": "Contract",
                   "balance_date": "Date",
                   "value_pos_mdo": "Portfolio_Value"
               }))

    df_port = df_port.sort_values(["Contract", "Date"]).reset_index(drop=True)

    return df_port

def clean_movements_data(df_mov):
    """
    Clean movements data by filtering deposits and withdrawals.
    Returns a clean dataframe with contract, description, amount, and date.
    """
    df_mov = df_mov.drop_duplicates()
    df_mov = df_mov[["contract", "description", "movement_import", "operation_date"]]
    df_mov["movement_import"] = pd.to_numeric(df_mov["movement_import"], errors="coerce")
    df_mov = df_mov[df_mov["contract"].astype(str).isin(CONTRATOS)]

    depositos_mask = df_mov["description"].str.contains("Depósito|Aportación", case=False, na=False)
    retiros_mask   = df_mov["description"].str.contains("Retiro|Salida", case=False, na=False)

    df_dep_ret = df_mov[depositos_mask | retiros_mask].copy().reset_index(drop=True)

    df_dep_ret = df_dep_ret.rename(columns={
        "contract": "Contract",
        "description": "Description",
        "movement_import": "Movement_Import",
        "operation_date": "Operation_Date"
    })

    return df_dep_ret

def MWRR(balance_df, movements_df, contract):
    """
    Calculate Money Weighted Return for a specific contract using pyxirr.
    
    Args:
        balance_df: Clean balance dataframe
        movements_df: Clean movements dataframe  
        contract: Contract ID to calculate return for
    
    Returns:
        Money Weighted Return as a percentage
    """
    try:
        # Filter data for the specific contract
        contract_balance = balance_df[balance_df['Contract'] == contract].copy()
        contract_movements = movements_df[movements_df['Contract'] == contract].copy()
        
        if len(contract_balance) == 0:
            return None
        
        # Sort by date (data is already sorted by day)
        contract_balance = contract_balance.sort_values('Date')
        contract_movements = contract_movements.sort_values('Operation_Date')
        
        # Create cash flows for MWRR
        cash_flows = []
        cash_flow_dates = []
        
        # Add all movements (deposits are negative, withdrawals are positive)
        for _, movement in contract_movements.iterrows():
            amount = movement['Movement_Import']
            if pd.isna(amount):
                continue
                
            if 'Depósito' in movement['Description'] or 'Aportación' in movement['Description']:
                cash_flows.append(-amount)  # Negative for deposits (money going out)
                cash_flow_dates.append(movement['Operation_Date'])
            elif 'Retiro' in movement['Description'] or 'Salida' in movement['Description']:
                cash_flows.append(amount)  # Positive for withdrawals (money coming in)
                cash_flow_dates.append(movement['Operation_Date'])
        
        # Add final portfolio value as positive cash flow
        final_value = contract_balance.iloc[-1]['Portfolio_Value']
        if not pd.isna(final_value):
            cash_flows.append(final_value)
            cash_flow_dates.append(contract_balance.iloc[-1]['Date'])
        
        # Convert dates to datetime if they're not already
        converted_dates = []
        for date in cash_flow_dates:
            if isinstance(date, str):
                # Try to parse as datetime
                try:
                    converted_date = pd.to_datetime(date)
                    converted_dates.append(converted_date)
                except:
                    return None
            else:
                converted_dates.append(date)
        
        # Calculate XIRR using pyxirr
        if len(cash_flows) < 2:
            return None
            
        mwrr = xirr(converted_dates, cash_flows)
        
        if mwrr is None:
            return None
        
        # Convert to percentage
        return mwrr * 100
            
    except Exception as e:
        return None

# Load the data
print("Loading data from data_actividad.xlsx...")
try:
    # First, let's check what sheets are available
    excel_file = pd.ExcelFile('data_actividad.xlsx')
    print(f"Available sheets: {excel_file.sheet_names}")
    
    # Read the Excel file with both sheets
    movements_df = pd.read_excel('data_actividad.xlsx', sheet_name='movements')
    balance_df = pd.read_excel('data_actividad.xlsx', sheet_name='balances')
    
    print("Data loaded successfully!")
    print(f"Movements data shape: {movements_df.shape}")
    print(f"Balance data shape: {balance_df.shape}")
    
    # Display column names to understand the structure
    print("\nMovements columns:")
    print(movements_df.columns.tolist())
    print("\nBalance columns:")
    print(balance_df.columns.tolist())
    
    # Clean the data using our functions
    print("\n" + "="*50)
    print("CLEANING DATA")
    print("="*50)
    
    # Clean balance data
    clean_balance = clean_balance_data(balance_df)
    print(f"Clean balance data shape: {clean_balance.shape}")
    
    # Clean movements data
    clean_movements = clean_movements_data(movements_df)
    print(f"Clean movements data shape: {clean_movements.shape}")
    
    # Display sample data
    print("\nMovements limpios:")
    print(clean_movements.head())
    
    print("\nBalances limpios:")
    print(clean_balance.head())
    
    # Export to CSV files
    clean_movements.to_csv("depositos_retiros.csv", index=False, encoding="utf-8-sig")
    clean_balance.to_csv("valor_portafolio_diario.csv", index=False, encoding="utf-8-sig")
    print("\nData exported to CSV files successfully!")
    
    # Calculate MWRR for each contract
    print("\n" + "="*50)
    print("CALCULATING MONEY WEIGHTED RETURNS")
    print("="*50)
    
    contracts = ['20486403', '12861603', 'AHA84901']
    
    for contract in contracts:
        print(f"\nCalculating MWRR for contract {contract}...")
        
        # Check if contract exists in the data
        if contract in clean_balance['Contract'].values and contract in clean_movements['Contract'].values:
            mwrr = MWRR(clean_balance, clean_movements, contract)
            if mwrr is not None:
                print(f"Rendimiento cliente {contract}: {mwrr:.2f}%")
            else:
                print(f"Could not calculate MWRR for contract {contract}")
        else:
            print(f"Contract {contract} not found in the data")
    
except Exception as e:
    print(f"Error loading data: {e}")
    print("Please make sure the file 'data_actividad.xlsx' exists in the current directory.")
