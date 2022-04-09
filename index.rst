Welcome to iStox.net Documentation!
=======================================

Contents
--------

.. toctree::
   :maxdepth: 2
   :glob:

   index
   OptionBasics

Why iStox?
=======================================

- No infrastructure to manage like servers/laptops, power, internet outages etc.
- No need to learn coding or write programs.
- No need to pay for server costs or development costs.
- Just configure your rules and system will take care of your trades.

Algo Feature Bucket List
=======================================

Instruments and Segments
---------------------------
- Options (ex: BANKNIFTY 23 DEC 35000 CE)
- Futures (ex: NIFTY DEC FUT, INFY DEC FUT)
- Equities (ex: RIL, INFY)

Time Based Strategies
---------------------------
- Ability to enter on a predefined time (ex: 9:20 am, 3:15 pm)
- Ability to enter on a calendar day (Mon/Tue etc.)
- Abiliyy to enter on a relative day. Few examples -
  -  Enter on Expiry day (expiry can be on Wednesday because of holidays)
  -  Exit 1 day before weekly expiry
  -  Exit on Monthly Expiry day.
 
Positional Strategies
---------------------------
- Ability to carry forward your open position beyond intraday. For example - enter on Monday and exit on expiry day.
- You can define SLs for positional strategies which will be implemented on AMO basis until the SL is hit or position is exited.
- Ability to configure time to place SLs next day. Ex: Place SLs at 9:18 am

Candle and Technical Indicator Based Strategies
---------------------------
- Use candle data to enter, exit and define your stop losses or take profits.
- You have 100+ technical indicators to choose from like - SMA, RSI, MACD and many more.
- Treades will be taken once your predefined levels are met.
- Positions can be intraday or positional.
- You can also setup exit based on time along with candle as an entry. Ex: Enter when CLOSE is above SMA 14 but exit at 3:15 PM.
- You can also define your own time period to enter. Ex: Take trade only between 9:30 AM to 12:00 PM.
- Ability to write your own formula using OHLC data. Ex: Enter when (CLOSE > SMA_14 and OPEN >SMA_14

Option Premium Based Strategies
---------------------------
- Ability to take positions based on premiums. Ex: SELL OPTIONS at a premium of Rs. 75. Buy a hedge position of Rs. 10. etc.
- You can use this as a protection mechanism while selling.
- Ex: BUY NIFTY 50 WEEKLY EXPIRY or MONTHLY EXPIRY around Rs.25. Algo will find the appropriate strike price and takes a BUY position.

Option Greeks Based Strategies
---------------------------
- Ability to take trades based on Greeks like Delta, Gamma, Vega, RHO etc.
- SELL Options trading at a delta of 30 points.

Stop Loss Features
---------------------------
- % based stop loss (ex: Stop loss of price is below 25% of entry AVG price)
- Points based stop loss (ex: Stop loss of price is below Rs. 50 of entry AVG price)
- Combined stop loss - If you have multiple legs open, you can define combined stop loss. (ex: I have 2 SELL LEGS and my overall loss should be capped at 25%. Not per individual LEG). All legs will be Squaredoff when conidition is met. 
- Move Stop Loss to Cost Individual - Move stop loss to cost of price moves in favour of our open position.
  - BUY Entry: 100 Original SL: 75.
  - Price Moves to: 150. Change SL to: 100.
  - You can define price movement in % or points
- Move Stop Loss to Cost Combined - Move stop loss to cost of an open LEG when other LEG hits the stop loss.
  - We need atleast 2 LEGS for this to work.
  - Say we have 2 SELL positions of CE and PE. If one leg hits SL (say CE), PE leg SL will be moved to COST basis.
  - Not recommended to use for strategies with more than 2 LEGS. If 1 LEG hits SL, other legs will be moved to COST.
  - This will help to cap max loss when we have multiple legs in a strategy.

Stop Loss Breach (or Freak Trades)
---------------------------
- Market can move wildly during market open hours. This means AMO SL can be way below/above an acceptable level, so may not be triggered by the broker/exchange.
- Other scenario that can also happen is what is popularly known as freak trades which moves market to unanticipated levels. Which usually doesn't last longer than few minutes.
- To handle these scenarios we have SL Breach logic which runs in the background and monitors all stop losses placed. This algorithm checks price every 30 seconds, if price is above stop loss levels defined for 6 times, algo looks for open positions where SL is not triggered by broker and automatically Squared off positions to prevent huge loss in the event if price continues to rally against you.
-  We will allow users to configure number of 30 second checks before taking an action. Some users may prefer to wait longer but some may want to immediately take action.

Take Profit Features
---------------------------
- % based take profit (ex: Take profit of price is above 25% of entry AVG price)
- Points based take profit (ex: Take profit of price is above Rs. 50 of entry AVG price)
- Combined take profit/MTM - If you have multiple legs open, you can define combined take profit. (ex: I have 2 SELL LEGS and my overall profit should be atleast 25% or say Rs. 1000). All legs will be Squaredoff when conidition is met. 

Square Off Features
---------------------------
- Exit based on time
- Exit based on Stoploss 
- Exit based on Takeprofit 
- Exit based on Candle or Technical Indicator rules

PnL Reporting
---------------------------
- Daily MTM PNL report for all strategies managed by our system will be sent at 4:45 PM IST
- MTM PNL will be sent only for the trades where all entries and exits are handled by the algo.
- Any manual inetrventions or order rejections by the broker will be excluded from the PNL calculation. This is to avoid wrong PNL calculations.
- You will have access to the same PNL report on Web UI as well. 
- In the WebUI you can analyze strategy performance over different time periods like weeks, months etc.
- WebUI report also shows your portfolio growth over a period of time, drawdown periods etc.
 
Alerts and Notifications
---------------------------
- Daily MTM PNL Report
- Startegy execution (Entry and Exit)
- Order Errors (Any rejections by the broker)
- Logion Errors if credentials provided are not up to date
