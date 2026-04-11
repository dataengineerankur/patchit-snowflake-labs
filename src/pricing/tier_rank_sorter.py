
Returns records sorted highest-score-first.
"""
    return sorted(records, key=lambda r: float(r.score), reverse=True)