
Abbreviations & Commonly used terms
=======================================
- NSE :: National Stock Exchangehttps://github.com/vinitshetty/iStox_Docs
- BSE :: Bombay Stock Exchange 
  - More about regulators read in the link(https://zerodha.com/varsity/chapter/regulators/)
- NF :: Nifty 50, NF 1 lot size :: 50 as on date
  - The index reflects the general market trend for a period of time. The index is a broad representation of the country’s state of the economy. A stock market index that up indicates people are optimistic about the future. Likewise, when the stock market index is down, people are pessimistic about the future.
- Bnf :: Bank Nifty, BnF 1 lot size :: 25 as on date
- SP :: Strike Price 
- OI :: Option Interest
- IV :: Implied Volatility
- ATM :: At the Money Option
  - Ex: Suppose the  nifty spot price is 17767, nearest 50th  strike ie 17750CE/17750PE
  - ATM are calls and puts whose strike price is at or very near to the CMP
  - ATM options are most sensitive to changes in various risk factors
  - Including time decay and changes to implied volatility or interest rates
- OTM :: Out Of The Money Option
  - Any option that does not have an intrinsic value is classified as OTM option
  - Ex: Strikes greater than 17750CE for on call side and < 17750pe on Put side
- ITM :: In The Money Option
  - Any option that has an intrinsic value is classified ITM option.
  - To see the option chain  having all ATM,ITM & OTM strikes or to exctract strikes use the below link
  - (https://www.nseindia.com/option-chain?symbolCode=-10006&symbol=NIFTY&symbol=NIFTY&instrument=-&date=-&segmentLink=17&symbolCount=2&segmentLink=17)
- LTP :: Last Traded Price
- CMP :: Current Market Price 
- Market :: Market Price
- MTM :: Mark to Market
- Long :: upside(Usually buy orders)
- Short :: downside (Usually sell orders)
- Buy :: one who buy the perticular contract
- Sell :: one who sell the perticular contract
- Limit :: Limit Price (fixed)
- SL :: Stoploss
  - A stop-loss order protects you from an adverse movement in the market after initiating a position. Suppose you buy ITC at Rs.262.25 with an expectation that ITC will hit Rs.275 shortly. But instead, what if the price of ITC starts going down? We can first protect ourselves by defining the worst possible loss you are willing to take. For instance, in the example let us assume you don’t want to take a loss beyond Rs.255

  - This means you have gone long on ITC at Rs.262.25 and the maximum loss you are willing to take on this trade is Rs.6 (255). If the stock price drops down to Rs.255, the stop loss order gets active and hits the exchange, and you will be out of the loss-making position. As long as the price is above 255, the stop-loss order will be dormant.

  - A stop-loss order is a passive order. To activate it, we need to enter a trigger price. A trigger price, usually above the stop-loss price acts as a price threshold and only after crossing this price the stop-loss order transitions from a passive order to an active order.
  - Stop-loss and stop-limit orders can provide different types of protection for investors. Stop-loss orders can guarantee execution, but price fluctuation and price slippage frequently occur upon execution. ... Stop-limit orders can guarantee a price limit, but the trade may not be executed.
  
- SL-L :: Stoploss limit (Exit at a pre defined price by the user)
- SL-M :: Stoploss  market(Exit at CMP)
- MIS :: Market Intra Day Squareoff
  - As the name suggests, MIS orders are intraday orders and needs to be squared off during the same trading day. If the order is not squared off by the user     or converted into other order types, the RMS system shall automatically square off the order a few minutes before the market close.
- NRML :: Normal (Regular) carry over
- BEP :: Breakeven point
  -  The breakeven point (break-even price) for a trade or investment is determined by comparing the market price of an asset to the original cost; the        breakeven point is reached when the two prices are equal.
- Contract :: Weekly exp contract or Monthly exp contract
- Week expiry : Every thursday of the month
- Monthly expiry : last week (last thursday) of the month
- Straddle : SELL ATM CE SELL ATM PE
- Strangle : SELL OTM CE SELl OTM PE
- Gut : SELL ITM PE and SELL ITM CE 
- MSLC : Move SL to Cost
- Margin : Appx  funds required to buy/sell the perticular contract

 Strategies
======================
 Intra day
---------------------------
- These are the strategies where in we enter the trade on the same day and exit on the same day without carrying the positions to the next day.
- 
 STBT (sell today buy tomorrow)
---------------------------
- Sell Today Buy Tomorrow (STBT) is a facility that allows customers to sell the shares in the cash segment (shares which are not in his demat account) and buy them the next day.
- STBT is the reverse of BTST (Buy Today Sell Tomorrow).
- None of the brokers in India offers STBT in the cash market as it's not permitted. You cannot sell shares if you don't have them in your demat account as brokers can't guarantee if those shares will be available in the market tomorrow to buy. If shares are not available tomorrow to buy, the broker will get panelized by the exchange for not to deliver the shares to the initial buyer.
- 
 BTST (buy today sell tomorrow)
---------------------------
- BTST (Buy Today, Sell Tomorrow) is a facility that allows customers to sell shares before they are credited into a demat account or take the delivery of shares. The decision has to be made in 2 days.


 **Positional (carry overnight)**
---------------------------
   -In this, sell ATM SP at a particular time with predefined SL
    
    -If SL not hit then, carry the position to next day and exit at predefined time
   
    -Ex: On monay at 1225 sell ATM straddle with SL of 36%
    -Assume spot at 17435, Then sell 17450CE & 17450PE of current week expiry,SL @36%
    -If both SL hit then strategy is closed automatically, If not, then either one or both legs are carried to tue.
    -In order to place SLL order, sys user/places  AMO orders at predefined time 
     by the user but before 09:00 am. In this we way we capture theta/time decay
    -If algo fails, ba cautious to place it manually,to avoid huge losses
    -There are days price comes back and we exit with positive slippage. The code checks  the open leg every 30sec and do 6 checks. We are allowing market to        settledown
    
 Expiry (Intrady only on Expiry day)
---------------------------
- There are strategies specially designed to capture max theta decay
- On expiry day movements are very wild, hence a wider stop losses are preferred.
- Ex: At 1110am sell ATM straddle with SL of 70%
- Assume spot at 17689, Sell 17700CE and 17700PE, if premium are 100 each, then SL would be 170
- If both SLL hit, strategy is closed. If not capture 

 Bull call spread
---------------------------
- Amongst all the spread strategies, the bull call spread is one the most popular one.
- The strategy comes handy when you have a moderately bullish view on the stock/index.
- The bull call spread is a two leg spread strategy traditionally involving ATM and OTM options.
- However you can create the bull call spread using other strikes as well.
-To implement the bull call spread
- Buy 1 ATM call option (leg 1)
- Sell 1 OTM call option (leg 2)
- When you do this ensure –
  - All strikes belong to the same underlying
  - Belong to the same expiry series
  - Each leg involves the same number of options
  - For example –
  - Date – 01 Nov 2021
  - Outlook – Moderately bullish (expect the market to go higher but the expiry around the corner could limit the upside)
  - Nifty Spot – 17846
  - ATM – 17850 CE, premium – Rs.80/-
  - OTM – 17900 CE, premium – Rs.30/-

 Bear call spread
---------------------------

 Short on Butterfly
---------------------------

 Calender spreads 
---------------------------

 Intra Day time based strategies 
============================================
- Selling ATM call and put options with same SP
- keep SLL wrt sold avg price & exit at a particular time of the day
- SLL may be in % or point based
- Ex: if Bnf spot is 34451 at 11.10am then ATM strike is 34500
- Sell 34500ce and 34500pe at 11.10am with SLL of 32% for each strike
- How to keep SLL(Stop Loss Limit) order-
  - Suppose 34500CE & PE have premium of  RS 100 each
  - Then SLL would be 132 for each leg, if strike premium moves and hits 132, that leg order is closed
  - In point based: If 40 points stop loss, then exit at 140 on each leg
  - If SLL order is triggered, then that that particular order is closed.
  - If both leg SLL orders triggered then we are out of the strategy.
  - Then next entry will be next day only
  
 Candle based strategies 
============================================
- In this strategy take entries based on colour, strength, close price or open price etc of the candles
- Ex:  If there are 2 consecutive same colour candles occur for the 1st time in the day within 11.45am
  - Enter ATM straddles with % based SLL and close position by 3.05pm or 3.10pm
- Ex1: 2 red color candles (30min TF) occur at 10.45 and 1115then place ATM straddle
  - Before 1045 we donot have 2 same consecutive color candles 
- Another strategy ORB based ie Open Range Breakout
  - Ex2: Apply ORB indicator in trading view in 30 min TF
    -If any candle closes above or below ORB line , sell/buy ATM strike of that candle closing
    -If suppose 1st candle(0915 to 0945) H-35000 & L 34900
    -Then next candle closes at 35087, then buy 35100CE or sell 35100PE
    
 Fixed profit MTM strategies
===================================
- Enter straddles/strangle with % or point based SLL 
- If we see certain profit (ex:2000INR) exit the trade or carry trade till 3.10pm

 STBT Strategies 
===================================
- Enter straddle/strangles wrt time 
- Keep % based SLL on sold avg price 
- Exit will be either SL hit or time based exit

 Positional Strategies 
===================================
- Take positions wrt time straddles/Strangles
- Keep % based SLL on sold avg price
- Entry day : (exp-2 or exp-3 or exp-4) based on the backtest done by the user
- Exit either SL or on EXP day

For More Information
======================
- Commonly used jargons(https://zerodha.com/varsity/chapter/commonly-used-jargons/)
- Options Basic (https://zerodha.com/varsity/module/option-theory/)
