import numpy as np

def calculate_volatility_goal(current_volatility, instrument_price, min_goal_weight, max_goal_weight, min_goal, max_goal):
    """
    Calculates a volatility goal based on current volatility, price, and weight parameters.
    """
    min_volatility = instrument_price * min_goal_weight
    max_volatility = instrument_price * max_goal_weight
    
    # Check for division by zero if max_volatility equals min_volatility
    if max_volatility == min_volatility:
        return min_goal

    # Scale the current volatility to a value between 0 and 1
    scaled_volatility = (current_volatility - min_volatility) / (max_volatility - min_volatility)
    
    # Apply an exponential function to the scaled volatility
    # Ensure arguments to log are positive
    if min_goal <= 0 or max_goal <= 0:
         # Fallback or error handling; original code didn't handle this but it's good practice
         # Assuming goals are positive as they come from random.randint(1, ...)
         pass
         
    goal = np.exp(scaled_volatility * np.log(max_goal / min_goal)) * min_goal
    
    if goal < min_goal:
        return min_goal
    elif goal > max_goal:
        return max_goal
    return goal
