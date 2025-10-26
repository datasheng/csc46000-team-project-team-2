port_mean #mean of portfolio
port_stdev #standard deviation of portfolio

for i in range(num_simulations):
    #induced randomness in stock spikes (downwards or upwards), will produce a dice roll like array of random spikes
    spikes = np.random.normal(0, 1, days)

    #GBM formula Tomorrow's Price = Current Price * e^(drift+spike)
    daily_returns = np.exp((port_mean - 0.5 * port_stdev**2) + port_stdev * spikes)

    #initial investment would be our starting value, multiplied by the cumulative product of all growth factors we simulated regarding the daily returns
    price_path = initial_investment * np.cumprod(daily_returns)

    #result array
    simulation_results[:, i] = price_path
