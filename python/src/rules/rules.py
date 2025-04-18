def fraud_status(amount, UCL, score, speed):
    """
    Determines the fraud status based on amount, UCL, score, and speed.

    Parameters:
    - amount: Transaction amount
    - UCL: Upper Control Limit
    - score: Credit score
    - speed: Transaction speed

    Returns:
    - "fraud" if any of the conditions are met
    - "genuine" otherwise
    """
    try:
        amount = float(amount)
        UCL = float(UCL)
        score = float(score)
        speed = float(speed)
    except ValueError:
        return "error"  # Or handle the error as appropriate

    if amount > UCL or score < 200 or speed > 900:
        return "fraud"
    else:
        return "genuine"

