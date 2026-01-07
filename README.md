# FlexPower Quant Challenge - Complete Solutions



This repository summarizes my work on the FlexPower quant challenge with extensions. The focus is on building a practical, robust approach to identifying profitable day-ahead (DA) to intraday (ID) opportunities, then in making the results accessible via a dashboard.

### What was completed
- **Quant Challenge Tasks 1 & 2**
  - Implemented and delivered the required solutions except 2.6.
  - For challenge 2.6 about the BESS, please refer to repo [BESS Optimizer](https://github.com/c-metz/bess-optimizer)

- **Additional work (adapted from other challenges, mostly OpsData)**
  - **Terminal PnL reporter**: lightweight reporting of strategy performance and key metrics.
  - **Interactive Streamlit dashboard**: exploratory analysis and visual monitoring of results.

### Key result
A simple machine learning approach is chosen. An XGBoost model is retrained every day on recent data to make predictions about what trades to make. XGBoost is chosen because of its fast training and inference even for many thousands of features, as well as robust classification skills. The achieved PnL for this approach stands at 12.8m EUR, the win rate at 62.8%, avg PnL per trade at 451.8 EUR. Two advantages of XGBost are its explainability, as well as the possibility to steer risk appetite via its classification probability - more in the next section.

### Modeling approach
Instead of point forecasting, a classification approach is used:
- Output: classification probability between 0 and 1 converted to 0/1 decision signal (i.e., take trade vs. do not take trade)
- Motivation: reduce complexity by making this a binary problem -> directly optimize for decision and reduce sensitivity to noisy price-level predictions.
- Importantly, classification models like XGBoost return a probability for classification -> this allows to choose a threshold starting from which one wants to trust the model. I.e., we can steer risk appetite by setting this threshold.

## Next improvements
- **More data**
  - Weather, load, spot prices, market projections/forecasts, and additional fundamental drivers.
- **More advanced model families**
  - Hyperparameter optimizaton of XGBoost, other tree-based models, deep neural networks, sequence models (LSTM/GRU), and model ensembles of all those models.

## Tooling note
For visualization, dashboards, and implementation sanity checks, I used GitHub Copilot in VS Code (primarily Claude Sonnet 4.5 via ask/agentic mode) and cross-checked with OpenAI's Codex. I deliberately avoided relying on AI for critical modeling logic like feature/label design because LLMs can miss subtle time-series leakage risks, such as using intraday information from the future to make a forecast in the present.

## Guide

If you want to go through task 2 step by step, please refer to task2_analysis.ipynb.


**Quick Start:**
```
pip install -r requirements.txt
python convert_trades.py       # Load 2021 trades
python task1_api.py            # API at localhost:8000
python task3_report.py strategy_ml_daily 2021-06-15  # Terminal report - insert YYYY-MM-DD of interest at the end
streamlit run task4_dashboard.py   # Dashboard at localhost:8501
```




# ORIGNAL README
# FlexPower Quant Challenge

* [Background](#background)
* [Task 1: Minimal Reporting Tool](#task-1-minimal-reporting-tool)
  * [Overview](#overview)
  * [Setup](#setup)
  * [Task 1.1: Total Buy/Sell Volume Calculation](#task-11-total-buy-sell-volume-calculation)
  * [Task 1.2: Strategy PnL Calculation](#task-12-strategy-pnl-calculation)
  * [Task 1.3: API for Strategy PnL](#task-13-api-for-strategy-pnl)
* [Task 2: Data Analysis and Building a Trading Strategy](#task-2-data-analysis-and-building-a-trading-strategy)
  * [Overview](#overview-1)
  * [Setup](#setup-1)
  * [Task 2.1: Wind/PV Power Forecast Analysis](#task-21-windpv-power-forecast-analysis)
  * [Task 2.2: Average Wind/Solar Production over 24 Hours](#task-22-average-windsolar-production-over-24-hours)
  * [Task 2.3: Average Value of Wind/Solar Power](#task-23-average-value-of-windsolar-power)
  * [Task 2.4: Days with Highest and Lowest Renewable Energy Production](#task-24-days-with-highest-and-lowest-renewable-energy-production)
  * [Task 2.5: Weekday vs Weekend Price Analysis](#task-25-weekday-vs-weekend-price-analysis)
  * [Task 2.6: Battery Revenue Calculation](#task-26-battery-revenue-calculation)
  * [Task 2.7: Trading Strategy Development](#task-27-trading-strategy-development)
* [Submission Instructions](#submission-instructions)


## Background
This challenge is meant to give you a taste of the type of problems our quants and developers have to solve on a daily basis. It helps you decide if you might have fun working with us. It is also an opportunity to demonstrate your technical/statistical/data skills and the ability to understand and work with our domain. It includes two tasks: In the first task you build a minimal reporting tool for a small trade database and expose them as an API. In the second task you do some energy data analysis and finally build your own trading strategy. 

Spend as much time as you want on the challenge to produce something you are proud of. The intended time is a couple of hours.

If you can think of any other cool features that you could implement, go for it! Just be sure to describe them in the readme.


## Task 1: Minimal Reporting tool

### Overview
Energy trading happens in an **exchange**, a market where traders working for energy producers 
(solar plants, nuclear power plants, ...) and consumers (B2C energy providers, big 
energy consuming industries like steel and trains...) submit orders to buy and sell energy.
One of the major exchanges in Europe is called [**EPEX**](https://en.wikipedia.org/wiki/European_Power_Exchange).

These orders consist of a quantity (in Megawatt) over a predefined period of
time, called **delivery period** (for example between 12:00, the **delivery start** and 13:00, 
the **delivery end**, on a given day) and for a given price per megawatt hour (referred to as mwh).

The orders are structured within orderbooks divided in bid (buy orders) and ask (sell orders).
![img.png](comtrader_snip.png)


If the prices of two orders with opposite sides match, i.e the buy price is higher than the sell price, 
then, a trade is generated. 
For example if the orderbook contains an order to sell 10 mw for 10 euros/mwh and another trader 
submits an order to buy 5 mw for 11 euros/mwh, the orders are matched by the exchange and a trade is 
generated for 5mw at 10 euros/mwh.

Those trades are saved by flex power and used to compute various indicators on the performance of our trading strategies. 
The first task bases on a collection of such trades and tries to implement a minimal reporting tool for different trading strategies.

The task models a trade as follows:
- Trade:
  - id: integer
  - price: float
  - quantity: integer
  - side: string, buy or sell
  - strategy_id: string

## Setup
The repository contains a sqlite file `trades.sqlite`, containing a database named `trades`.

The database contains a table called `epex_12_20_12_13`, with all trades made by flex power traders 
 on the EPEX exchange for the delivery period 12:00 to 13:00 on 2022-12-20.

The schema is the following
```sqlite
id TEXT PRIMARY KEY,
quantity INTEGER NOT NULL,
price REAL NOT NULL,
side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
strategy TEXT NOT NULL
```

### Task 1.1: 
Write a function that computes the total buy volume for flex power, another that computes the total sell volume.
```python

def compute_total_buy_volume(*args, **kwargs) -> float:
    pass

def compute_total_sell_volume(*args, **kwargs) -> float:
    pass
```

### Task 1.2: 
Write a function that computes the PnL (profit and loss) of each strategy in euros. It's defined as the sum of the incomes 
realized with each trade.

If we sell energy, our income is `quantity * price` since we got money for our electricity. If we buy energy, our income is `-quantity * price`.
```python
def compute_pnl(strategy_id: str, *args, **kwargs) -> float:
    pass
```
This function should return 0 it there are no trades that correspond to the strategy id.


### Task 1.3: 
Expose the function defined in the second task as an entrypoint of a web application. 

Here is the corresponding API definition.
```yaml
swagger: "2.0"
info:
  title: Energy Trading API
  version: 1.0.0
host: api.example.com
basePath: /v1
schemes:
  - https
paths:
  /pnl/{strategy_id}:
    get:
      summary: Returns the pnl of the corresponding strategy.
      parameters:
        - in: path
          name: user_id
          required: true
          type: string
          description: string identifier of a strategy.
      produces:
        - application/json
      responses:
        200:
          description: A PnL data object.
          schema:
            type: object
            properties:
              strategy:
                type: string
                example: my_strategy
              value:
                type: float
                example: 100.0
              unit:
                type: string
                example: euro
              capture_time:
                type: string
                example: "2023-01-16T08:15:46Z"
```


## Task 2: Data analysis and building a trading strategy

### Overview

Power prices depend on external factors, most importantly the (forecasted) infeed from variable renewable energy sources PV and wind. In this task you are supposed to do some data analysis, get to know this correlation and finally build your own trading strategy based on wind and pv forecasts. The trading strategy is task 2.7, try to spend most of your time on that one. It is a open task so get creative here. We are not looking for the one correct answer but rather train of thought and logical reasoning. We encourage you though to present clean results, nice graphs and generally good work.

Here's some information on the dataset to get you started.

- A big part of power trading takes places in 15 minute windows, so does this dataset. As an example, the data of the column with timestamp "02.03.21 19:30" tells you the forecasts and prices for the quarter hour 19:30h-19:45h of that day.

- In case you are not familiar with the basic structure of the European wholesale power markets, here's a brief summary. Every day at 12:00h the day-ahead auction takes place, where power is traded for the 24 hours of the following day. For the example column "02.03.21 19:30", the date of the day-ahead auction is therefore 01.03.21 12:00h. 3 hours later, at 15:00h, the Intraday markets open, where then power can be traded in 15 minute blocks right until 5 minutes before delivery start. For the example column "02.03.21 19:30", power can therefore be traded until 02.03.21 19:25h. Finally, all remaining positions will then be closed against the "Imbalance Price" afterwards.

Following is a short exlainer of the supplied timeseries:

- "Wind Day Ahead Forecast" gives the total forecasted amount of wind power expected to be produced in Germany at given quarter. The day-ahead forecast is available before the day-ahead auction takes place, so at d-1, before 12:00h. 
 
- "Wind Intraday Forecast" gives the total forecasted amount of wind power expected to be produced in Germany at given quarter. The intraday forecast is the last available forecast before the intraday markets close.

- "PV Day Ahead Forecast" gives the total forecasted amount of PV power expected to be produced in Germany at given quarter. The day-ahead forecast is available before the day-ahead auction takes place, so at d-1, before 12:00h. 
 
- "PV Intraday Forecast" gives the total forecasted amount of PV power expected to be produced in Germany at given quarter. The intraday forecast is the last available forecast before the intraday markets close.

- "Day Ahead price hourly" gives the realized prices on the Day-ahead auction for that hour.
  
- "Intraday Price Quarter Hourly" gives the realized Intraday prices on the quarterhourly markets.

- "Intraday Price Hourly" gives the realized Intraday prices on the hourly markets.

- "Imbalance Price Quarter Hourly" gives the realized Imbalance price for that quarter.

### Setup

This repository contains an Excel file called "analysis_task_data.xlsx". Feel free to import the data in Python, R, Matlab, whatever you feel most comfortable with. This can be done in Excel but does not need to be done in Excel.


### Task 2.1:
How much Wind/PV Power was forecasted to produced in German in 2021 [in MWh] on Day Ahead (da) and on Intraday (id). Hint: Be careful: you have values in MW on a quarter hourly basis, think how this translates into hourly values.

### Task 2.2:
Show the average Wind/Solar production for 2021 over a 24h period for Intraday and Day Ahead (4 lines in one graph).

### Task 2.3:
What was the average value [in EUR/MWh] for Wind/Solar Power in 2021 using the da forecast and using da h prices? The average value is defined as the average hourly value that a Wind/PV farm owner would have received for their product. Is the average value of Wind and PV higher or lower than the average da price? Why could it be higher/lower?

### Task 2.4:
Find the Day with the highest renewable energy production and with the lowest renewable energy production in 2021. What was the average Day Ahead Price levels on these days? How do you explain the difference in prices?

### Task 2.5:
What is the average hourly da price during week days vs during weekends. Why do you think average prices may differ?

### Task 2.6:
How much revenue would you generate with a battery with a capacity of 1 MWh which you can fully charge and fully discharge (1 Cycle) every day in 2021? Think about when you would charge and when you would discharge and apply this rule for each day of the year.

### Task 2.7:
Come up with a trading strategy that makes money between the day ahead hourly prices and the intraday hourly prices. A strategy could be something like, always buy hour 19-20 on day ahead and sell it on intraday. You can look at certain times, weekdays, seasons, production levels of wind and solar. Your strategy can have a few input paramters such as time, renewable production etc, and then a decision output between two prices. I.e. when do you want to go long and short. Show the cumulative performance of this strategy with a 100 MW position. Show your results and quickly explain your reasoning of why you think this strategy might be a good idea and why it does or does not work.


## Submission Instructions
Create a Github repository containing your solution and provide us with access so that we can review your 
code.

Feel free to add notes about technology choices and design decisions, as well as anything that we should
keep in mind when reviewing your code.

Ideally, the deliverable should be in python 3.*.
