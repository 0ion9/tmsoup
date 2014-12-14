def get_config(cursor, key, default=None):
    val = cursor.execute('select value from setting where name = ?', 
          (key,)).fetchone()
    if val is None:
        return default
    return val[0]
