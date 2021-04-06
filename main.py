from Flask import Flask, render_template
from google.cloud import bigquery

app = Flask(__name__)

client = bigguery.Client()

@app.route('/')
def tasks():
    return render_template(index.html)
@app.route('/task1')
def task1():
    query = """
    SELECT 
    time_ref, SUM(value) as trade_value, FROM `clouda1-309323.gsquarterlySeptember20.gsquarterlySeptember20`
    GROUP BY time_ref, value
    ORDER BY SUM(value) DESC 
    LIMIT 
    10
    """
    query_task = client.query(query)

    return render_template('task1.html')
    
@app.route('/task2')
def task2():
    query = """
    SELECT
    COUNTRY.country_label, product_type,
    SUM(CASE account
      WHEN 'Imports' THEN value
      WHEN 'Exports' THEN -value
     END
    ) AS trade_deficit_value,
    status
    FROM
    `clouda1-309323.gsquarterlySeptember20.gsquarterlySeptember20`AS GS,
    `clouda1-309323.country_classification.country_classification` AS COUNTRY
    WHERE
    (time_ref >  201400
    OR time_ref <  201700)
    AND status LIKE 'F'
    AND product_type LIKE 'Goods'
    AND GS.country_code = COUNTRY.country_code
    GROUP BY
    COUNTRY.country_label,
    status, product_type
    ORDER BY
    trade_deficit_value DESC
    LIMIT
     50
     """
  query_task = client.query(query)
return render_template('task2.html')
@app.route('/task3')
def task3():

    query = """
    SELECT
  SERVICE.service_label,
  SUM(CASE account
      WHEN 'Imports' THEN -value
      WHEN 'Exports' THEN value
  END
    ) AS trade_surplus_value
FROM
  `clouda1-309323.gsquarterlySeptember20.gsquarterlySeptember20`AS GS,
  `clouda1-309323.service_classification.services_classification` AS SERVICE,
  (
  SELECT
    time_ref,
    SUM(value) AS trade_value
  FROM
    `clouda1-309323.gsquarterlySeptember20.gsquarterlySeptember20`
  GROUP BY
    time_ref
  ORDER BY
    SUM(value) DESC
  LIMIT
    10) AS SUBQUERY1,
  (
  SELECT
  country_code,product_type,
  SUM(CASE account
      WHEN 'Imports' THEN value
      WHEN 'Exports' THEN -value
  END
    ) AS trade_deficit_value,
  status
FROM
  `clouda1-309323.gsquarterlySeptember20.gsquarterlySeptember20`
WHERE
  (time_ref >  201400
    OR time_ref <  201700)
  AND status LIKE 'F'
  AND product_type LIKE 'Goods'
GROUP BY
  country_code,
  status, product_type
ORDER BY
  trade_deficit_value DESC
LIMIT
  50) AS SUBQUERY2
WHERE
  (GS.time_ref = SUBQUERY1.time_ref)
  AND (GS.country_code = SUBQUERY2.country_code)
  AND GS.code = SERVICE.code
GROUP BY
  SERVICE.service_label
ORDER BY
  trade_surplus_value DESC
LIMIT
  30
  """

query_task = client.query(query)
return render_template('task3.html')

if __name__ == "__main__":
    app.run(host='localhost',debug=True)