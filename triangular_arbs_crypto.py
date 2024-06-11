import ccxt
import time
from datetime import datetime
import math
from prettytable import PrettyTable
import mysql.connector

# Create a table instance
combination_table = PrettyTable()
result_table = PrettyTable()
exchange = ccxt.binance({
    'apiKey': 'api_key',
    'secret': 'secret_key',
})


markets = exchange.fetchMarkets()
market_symbols = [market['symbol'] for market in markets]
# print(market_symbols)
print('No of market symbols :',len(markets))

def get_crypto_combinations(market_symbols, base):
    combinations = []
    for sym1 in market_symbols:
        sym1_token1 = sym1.split('/')[0]
        sym1_token2 = sym1.split('/')[1]
        if (sym1_token2 == base):
            for sym2 in market_symbols:
                sym2_token1 = sym2.split('/')[0]
                sym2_token2 = sym2.split('/')[1]
                if (sym1_token1 == sym2_token2):
                    for sym3 in market_symbols:
                        sym3_token1 = sym3.split('/')[0]
                        sym3_token2 = sym3.split('/')[1]
                        if ((sym2_token1 == sym3_token1) and (sym3_token2 == sym1_token2)):
                            # Define the table headers
                            combination_table.field_names = ["base", "intermediate", "ticker"]
                            # Add data to the table
                            combination_table.add_row([sym1_token2, sym1_token1,sym2_token1])

                            combination = {
                                'base': sym1_token2,
                                'intermediate': sym1_token1,
                                'ticker': sym2_token1,
                            }

                            combinations.append(combination)
    return combinations


wx_combinations_usdt = get_crypto_combinations(market_symbols, 'USDT')
print('No of crypto combinations:',len(wx_combinations_usdt))

def fetch_current_ticker_price(ticker):
    current_ticker_details = exchange.fetch_ticker(ticker)
    ticker_price = current_ticker_details['close'] if current_ticker_details is not None else None
    return ticker_price

def check_if_float_zero(value):
    return math.isclose(value, 0.0, abs_tol=1e-3)

def check_buy_buy_sell(scrip1, scrip2, scrip3, initial_investment):
    ## SCRIP1
    investment_amount1 = initial_investment
    current_price1 = fetch_current_ticker_price(scrip1)
    final_price = 0
    scrip_prices = {}

    if current_price1 is not None:
        buy_quantity1 = round(investment_amount1 / current_price1, 8)
        time.sleep(1)
        ## SCRIP2
        investment_amount2 = buy_quantity1
        current_price2 = fetch_current_ticker_price(scrip2)
        if current_price2 is not None:
            buy_quantity2 = round(investment_amount2 / current_price2, 8)
            time.sleep(1)
            ## SCRIP3
            investment_amount3 = buy_quantity2
            current_price3 = fetch_current_ticker_price(scrip3)
            if current_price3 is not None:
                sell_quantity3 = buy_quantity2
                final_price = round(sell_quantity3 * current_price3, 3)
                scrip_prices = {scrip1: current_price1, scrip2: current_price2, scrip3: current_price3}

    return final_price, scrip_prices


def check_buy_sell_sell(scrip1, scrip2, scrip3, initial_investment):
    ## SCRIP1
    investment_amount1 = initial_investment
    current_price1 = fetch_current_ticker_price(scrip1)
    final_price = 0
    scrip_prices = {}
    if current_price1 is not None and not check_if_float_zero(current_price1):
        buy_quantity1 = round(investment_amount1 / current_price1, 8)

        # TRY WITHOUT SLEEP IF THE EXCHANGE DOES NOT THROW RATE LIMIT EXCEPTIONS
        # time.sleep(1)
        ## SCRIP2
        investment_amount2 = buy_quantity1
        current_price2 = fetch_current_ticker_price(scrip2)
        if current_price2 is not None and not check_if_float_zero(current_price2):
            sell_quantity2 = buy_quantity1
            sell_price2 = round(sell_quantity2 * current_price2, 8)

            # TRY WITHOUT SLEEP IF THE EXCHANGE DOES NOT THROW RATE LIMIT EXCEPTIONS
            # time.sleep(1)
            ## SCRIP1
            investment_amount3 = sell_price2
            current_price3 = fetch_current_ticker_price(scrip3)
            if current_price3 is not None and not check_if_float_zero(current_price3):
                sell_quantity3 = sell_price2
                final_price = round(sell_quantity3 * current_price3, 3)
                scrip_prices = {scrip1: current_price1, scrip2: current_price2, scrip3: current_price3}
    return final_price, scrip_prices


def check_profit_loss(total_price_after_sell,initial_investment,transaction_brokerage, min_profit):
    apprx_brokerage = transaction_brokerage * initial_investment/100 * 3
    min_profitable_price = initial_investment + apprx_brokerage + min_profit
    profit_loss = round(total_price_after_sell - min_profitable_price,3)
    return profit_loss


def place_buy_order(scrip, quantity, limit):
    order = exchange.create_limit_buy_order(scrip, quantity, limit)
    return order


def place_sell_order(scrip, quantity, limit):
    order = exchange.create_limit_sell_order(scrip, quantity, limit)
    return order


def place_trade_orders(type, scrip1, scrip2, scrip3, initial_amount, scrip_prices):
    final_amount = 0.0
    if type == 'BUY_BUY_SELL':
        print('BUY_BUY_SELL')
        s1_quantity = initial_amount / scrip_prices[scrip1]
        print('BUY')
        print("This is the initial amout :",initial_amount,"These are prices : " ,scrip_prices[scrip1],'type' , scrip1)
        print('S1 Quantity',s1_quantity)
        # place_buy_order(scrip1, s1_quantity, scrip_prices[scrip1])

        s2_quantity = s1_quantity / scrip_prices[scrip2]
        print('BUY')
        print("This is the initial amout :", initial_amount, "These are prices : ", scrip_prices[scrip2],'type' , scrip2)
        print('S2 Quantity', s2_quantity)
        # place_buy_order(scrip2, s2_quantity, scrip_prices[scrip2])

        s3_quantity = s2_quantity
        print('SELL')
        print("This is the initial amout :", initial_amount, "These are prices : ", scrip_prices[scrip2],'type' , scrip2)
        print('S3 Quantity', s3_quantity)
        # place_sell_order(scrip3, s3_quantity, scrip_prices[scrip3])

    elif type == 'BUY_SELL_SELL':
        print('BUY_SELL_SELL')
        s1_quantity = initial_amount / scrip_prices[scrip1]
        print('BUY')
        print("This is the initial amout :", initial_amount, "These are prices : ", scrip_prices[scrip1] ,'type' , scrip1)
        print('S1 Quantity', s1_quantity)
        # place_buy_order(scrip1, s1_quantity, scrip_prices[scrip1])

        s2_quantity = s1_quantity
        print('SELL')
        print("This is the initial amout :", initial_amount, "These are prices : ", scrip_prices[scrip2],'type' , scrip2)
        print('S2 Quantity', s2_quantity)
        # place_sell_order(scrip2, s2_quantity, scrip_prices[scrip2])

        s3_quantity = s2_quantity * scrip_prices[scrip2]
        print('SELL')
        print("This is the initial amout :", initial_amount, "These are prices : ", scrip_prices[scrip2],'type' , scrip3)
        print('S3 Quantity', s3_quantity)
        # place_sell_order(scrip3, s3_quantity, scrip_prices[scrip3])
    return final_amount


def perform_triangular_arbitrage(scrip1, scrip2, scrip3, arbitrage_type, initial_investment,
                                 transaction_brokerage, min_profit):
    final_price = 0.0
    if (arbitrage_type == 'BUY_BUY_SELL'):
        # Check this combination for triangular arbitrage: scrip1 - BUY, scrip2 - BUY, scrip3 - SELL
        final_price, scrip_prices = check_buy_buy_sell(scrip1, scrip2, scrip3, initial_investment)

    elif (arbitrage_type == 'BUY_SELL_SELL'):
        # Check this combination for triangular arbitrage: scrip1 - BUY, scrip2 - SELL, scrip3 - SELL
        final_price, scrip_prices = check_buy_sell_sell(scrip1, scrip2, scrip3, initial_investment)

    profit_loss = check_profit_loss(final_price, initial_investment, transaction_brokerage, min_profit)

    if profit_loss > 2:
        # Define the table headers
        result_table.field_names = ["Time", "Arbitrage Type", "Scrip 1", "Scrip 2", "Scrip 3","Investment",'Returns ', "Profit/Loss"]

        # Add data to the table
        result_table.add_row([
            datetime.now().strftime('%H:%M:%S'),
            arbitrage_type,
            scrip1,
            scrip2,
            scrip3,
            initial_investment,
            final_price,
            round(final_price - initial_investment, 3)
        ])

        print(result_table)

        profit = final_price - initial_investment
        report_module(arbitrage_type, scrip1, scrip2, scrip3, initial_investment,final_price, profit)
        place_trade_orders(arbitrage_type, scrip1, scrip2, scrip3, initial_investment, scrip_prices)

    # if profit_loss > 0:
        # print(f"PROFIT-{datetime.now().strftime('%H:%M:%S')}:"
        #       f"{arbitrage_type}, {scrip1},{scrip2},{scrip3}, Profit/Loss: {round(final_price - initial_investment, 3)} ")
        # place_trade_orders(arbitrage_type, scrip1, scrip2, scrip3, initial_investment, scrip_prices)

def report_module(arbitrage_type, scrip1, scrip2, scrip3, initial_investment,final_price, profit):
    try:
        # Replace with your actual database credentials
        db_config = {
            "host": "localhost",
            "user": "root",
            "database": "crypto",
            "password": "password",  # Assuming no password
        }

        # Create a connection to the database
        connection = mysql.connector.connect(**db_config)

        # Create a cursor object to interact with the database
        cursor = connection.cursor()

        # Define the table creation query
        create_table_query = """
        CREATE TABLE IF NOT EXISTS crypto (
            id INT AUTO_INCREMENT PRIMARY KEY,
            current_time_eat DATETIME,
            arbitrage_type VARCHAR(255),
            scrip1 VARCHAR(255),
            scrip2 VARCHAR(255),
            scrip3 VARCHAR(255),
            initial_investment DECIMAL(10, 2),
            final_price DECIMAL(10, 2),
            profit DECIMAL(10, 2)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """

        # Execute the table creation query
        cursor.execute(create_table_query)

        current_time_eat = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Sample data to insert
        data_to_insert = {
            'current_time_eat':current_time_eat,
            "arbitrage_type":  arbitrage_type,
            "scrip1": scrip1,
            "scrip2": scrip2,
            "scrip3": scrip3,  # Replace with the actual time
            "initial_investment": initial_investment,
            "final_price": final_price,
            "profit": profit,
        }

        # SQL statement for insertion
        insert_query = """
        INSERT INTO crypto
        (current_time_eat,arbitrage_type, scrip1, scrip2, scrip3, initial_investment,  final_price, profit)
        VALUES
        (%(current_time_eat)s,%(arbitrage_type)s, %(scrip1)s, %(scrip2)s, %(scrip3)s, %(initial_investment)s, %(final_price)s, %(profit)s)
        """

        # Execute the insertion query
        cursor.execute(insert_query, data_to_insert)

        # Commit the transaction
        connection.commit()

        # Close the cursor and connection
        cursor.close()
        connection.close()
        print("data stored successfully")
    except Exception as e:
        print("Not stored", e)


INVESTMENT_AMOUNT_DOLLARS = 2
MIN_PROFIT_DOLLARS = 0.5
BROKERAGE_PER_TRANSACTION_PERCENT = 0.2

while True:
    try:
        for combination in wx_combinations_usdt:
            if combination in market_symbols:  # Fix: Check if the combination is in market_symbols
                base = combination['base']
                intermediate = combination['intermediate']
                ticker = combination['ticker']

                s1 = f'{intermediate}/{base}'  # Eg: BTC/USDT
                s2 = f'{ticker}/{intermediate}'  # Eg: ETH/BTC
                s3 = f'{ticker}/{base}'  # Eg: ETH/USDT

                # 1. Check triangular arbitrage for buy-buy-sell
                perform_triangular_arbitrage(s1, s2, s3, 'BUY_BUY_SELL', INVESTMENT_AMOUNT_DOLLARS,
                                             BROKERAGE_PER_TRANSACTION_PERCENT, MIN_PROFIT_DOLLARS)

                # 2. Check triangular arbitrage for buy-sell-sell
                perform_triangular_arbitrage(s3, s2, s1, 'BUY_SELL_SELL', INVESTMENT_AMOUNT_DOLLARS,
                                             BROKERAGE_PER_TRANSACTION_PERCENT, MIN_PROFIT_DOLLARS)

                time.sleep(1)
    except Exception as e:  # Fix: Specify the exception type you want to catch
        print(f"An error occurred: {e}")

# while (1):
#     for combination in wx_combinations_usdt:
#         base = combination['base']
#         intermediate = combination['intermediate']
#         ticker = combination['ticker']
#
#         s1 = f'{intermediate}/{base}'  # Eg: BTC/USDT
#         s2 = f'{ticker}/{intermediate}'  # Eg: ETH/BTC
#         s3 = f'{ticker}/{base}'  # Eg: ETH/USDT
#
#         # 1. Check triangular arbitrage for buy-buy-sell
#         perform_triangular_arbitrage(s1, s2, s3, 'BUY_BUY_SELL', INVESTMENT_AMOUNT_DOLLARS,
#                                      BROKERAGE_PER_TRANSACTION_PERCENT, MIN_PROFIT_DOLLARS)
#
#         # Check triangular arbitrage for buy-sell-sell
#         perform_triangular_arbitrage(s3, s2, s1, 'BUY_SELL_SELL', INVESTMENT_AMOUNT_DOLLARS,
#                                      BROKERAGE_PER_TRANSACTION_PERCENT, MIN_PROFIT_DOLLARS)
#         time.sleep(1)