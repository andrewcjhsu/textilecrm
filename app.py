import os
import psycopg2
import jinja2
from flask import Flask, redirect, request, render_template, url_for, session, flash
from urllib.parse import urlparse

app = Flask(__name__)
app.secret_key = 'I Hate Databases'


def get_db_connection():
    result = urlparse(os.environ['DATABASE_URL'])
    username = result.username
    password = result.password
    database = result.path[1:]
    hostname = result.hostname
    port = result.port
    connection = psycopg2.connect(
        database=database,
        user=username,
        password=password,
        host=hostname,
        port=port
    )

    return connection


@app.route('/<path:text>', methods=['GET', 'POST'])
def all_routes(text):
    if text.startswith('pages') or text.startswith('sections'):
        return render_template(text)


@app.route('/', methods=['GET'])
def hello_world():  # put application's code here
    conn = get_db_connection()
    # create a cursor
    cur = conn.cursor()

    # execute a statement
    print('PostgreSQL database version:')
    cur.execute('SELECT version()')
    # display the PostgreSQL database server version
    session["db_version"] = cur.fetchone()[0]
    # Top 10 Sales by customers
    cur.execute("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, c.customer_name, c.type, c.class
                    FROM crm_opportunity o, crm_customer c   
                    WHERE o.customer_key = c.customer_key 
                    GROUP BY c.type, c.class, c.customer_name
                    ORDER BY rev DESC
                    limit 10;""")
    session["customer_sales"] = cur.fetchall()

    # Top Sales by each product category
    cur.execute("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, p.product_name, p.category, p.description
                    FROM crm_opportunity o, crm_product p  
                    WHERE o.product_key = p.product_key 
                    GROUP by p.product_name, p.category, p.description
                    HAVING sum (o.deal_amount_aftertax) >= ALL
	                (select sum (o1.deal_amount_aftertax)
	                    FROM crm_opportunity o1, crm_product p1
	                    WHERE o1.product_key = p1.product_key
	                    AND p.category = p1.category
	                    GROUP by p1.product_name)
                    ORDER BY rev DESC;""")
    session["product_sales"] = cur.fetchall()


    # Average Sales by Users b_unit and title
    cur.execute("""SELECT round(avg (o.deal_amount_aftertax),0) as rev, u.b_unit,u.title
                        FROM crm_opportunity o, crm_user u
                        WHERE o.user_key = u.user_key
                        Group BY u.b_unit, u.title
                        ORDER BY rev DESC
                        limit 10;""")
    session["predictive_sales"] = cur.fetchall()
#
#     # Most Profitable Campaign of each type
    cur.execute("""SELECT round(SUM(o.deal_amount_aftertax/c.cost),0) as profit,  c.cost, c.campaign_name, c.type
                    FROM crm_opportunity o, crm_campaign c 
                    WHERE o.campaign_key = c.campaign_key 
                    Group BY c.campaign_name, c.type, c.cost
					HAVING SUM(o.deal_amount_aftertax/c.cost) >= ALL (
						SELECT SUM(o1.deal_amount_aftertax/c1.cost)
						FROM crm_opportunity o1, crm_campaign c1 
                    	WHERE o1.campaign_key = c1.campaign_key 
						AND c.type = c1.type
						GROUP BY c1.campaign_name
					)
					ORDER BY profit desc;""")
    session["campaign_roi"] = cur.fetchall()
#
    # close the communication with the PostgreSQL
    cur.close()
    return render_template('index.html', version=session["db_version"],
                           customer_sales=session.get("customer_sales"),
                           product_sales=session.get("product_sales"),
                           predictive_sales=session.get("predictive_sales"),
                           campaign_roi=session.get("campaign_roi"))
@app.route('/customer_sales', methods=['POST'])
def query_customer_sales():  # template for some query
    if request.method == 'POST':
        Month_choice = request.form.get('table-choice')
        print(Month_choice)
        conn = get_db_connection()
        cur = conn.cursor()
        if Month_choice == 'Q1':
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, c.customer_name, c.type, c.class
                            FROM crm_opportunity o, crm_customer c, crm_update d 
                            WHERE o.customer_key = c.customer_key 
							AND o.date_key = d.date_key 
							AND d.quarter = '1'
                            GROUP BY c.type, c.class, c.customer_name
                            ORDER BY rev DESC
                            limit 10;"""))  # insert query here
        elif Month_choice == 'Q2':
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, c.customer_name, c.type, c.class
                            FROM crm_opportunity o, crm_customer c, crm_update d 
                            WHERE o.customer_key = c.customer_key 
							AND o.date_key = d.date_key 
							AND d.quarter = '2'
                            GROUP BY c.type, c.class, c.customer_name
                            ORDER BY rev DESC
                            limit 10;"""))
        elif Month_choice == 'Q3':
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, c.customer_name, c.type, c.class
                            FROM crm_opportunity o, crm_customer c, crm_update d 
                            WHERE o.customer_key = c.customer_key 
							AND o.date_key = d.date_key 
							AND d.quarter = '3'
                            GROUP BY c.type, c.class, c.customer_name
                            ORDER BY rev DESC
                            limit 10;"""))
        elif Month_choice == 'Q4':
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, c.customer_name, c.type, c.class
                            FROM crm_opportunity o, crm_customer c, crm_update d 
                            WHERE o.customer_key = c.customer_key 
							AND o.date_key = d.date_key 
							AND d.quarter = '4'
                            GROUP BY c.type, c.class, c.customer_name
                            ORDER BY rev DESC
                            limit 10;"""))  # insert query here
        else:
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, c.customer_name, c.type, c.class
                            FROM crm_opportunity o, crm_customer c   
                            WHERE o.customer_key = c.customer_key 
                            GROUP BY c.type, c.class, c.customer_name
                            ORDER BY rev DESC
                            limit 10; """))  # insert query here
        # point to existing session and modify
        session["customer_sales"] = cur.fetchall()  # fetches query and put into object
        session.modified = True

        cur.close()  # closes query
        conn.close()  # closes connection to db
    return render_template('index.html', version=session["db_version"],
                           customer_sales=session.get("customer_sales"),
                           product_sales=session.get("product_sales"),
                           predictive_sales=session.get("predictive_sales"),
                           campaign_roi=session.get("campaign_roi"))

@app.route('/product_sales', methods=['POST'])
def query_product_sales():  # template for some query
    if request.method == 'POST':
        Month_choice = request.form['table-choice']
        conn = get_db_connection()
        cur = conn.cursor()
        if Month_choice == '2021':
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, p.product_name, p.category, p.description
                            FROM crm_opportunity o, crm_product p  
                            WHERE o.stage = 'Closed_Won' 
                            AND o.close_date < '2022-01-01'
                            AND o.product_key = p.product_key 
                            GROUP BY p.category, p.product_name, p.description
                            ORDER BY rev desc
                            limit 10;"""))  # insert query here
        elif Month_choice == '2022':
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, p.product_name, p.category, p.description
                            FROM crm_opportunity o, crm_product p  
                            WHERE o.stage = 'Closed_Won' 
                            AND o.close_date >= '2022-01-01'
                            AND o.product_key = p.product_key 
                            GROUP BY p.category, p.product_name, p.description
                            ORDER BY rev desc
                            limit 10;"""))  # insert query here
        else:
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, p.product_name, p.category, p.description
                            FROM crm_opportunity o, crm_product p  
                            WHERE o.stage = 'Closed_Won'  
                            AND o.product_key = p.product_key 
                            GROUP BY p.category, p.product_name, p.description
                            ORDER BY rev desc
                            limit 10;"""))  # insert query here
        # point to existing session and modify
        session["product_sales"] = cur.fetchall()  # fetches query and put into object
        session.modified = True
        cur.close()  # closes query
        conn.close()  # closes connection to db
    return render_template('index.html', version=session["db_version"],
                           customer_sales=session.get("customer_sales"),
                           product_sales=session.get("product_sales"),
                           predictive_sales=session.get("predictive_sales"),
                           campaign_roi=session.get("campaign_roi"))


@app.route('/predictive_sales', methods=['POST'])
def query_predictive_sales():  # template for some query
    if request.method == 'POST':
        Month_choice = request.form['table-choice']
        conn = get_db_connection()
        cur = conn.cursor()
        if Month_choice == '2021':
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax * o.win_rate/100),0) as rev, u.user_name, u.b_unit,u.title
                            FROM crm_opportunity o, crm_user u
                            WHERE o.user_key = u.user_key
                            AND o.stage != 'Closed_Won'
                            AND o.create_date < '2022-04-01'
                            Group BY u.b_unit, u.title, u.user_name
                            ORDER BY rev DESC
                            limit 10;"""))  # insert query here
        elif Month_choice == '2022Q1':
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax * o.win_rate/100),0) as rev, u.user_name, u.b_unit,u.title
                            FROM crm_opportunity o, crm_user u
                            WHERE o.user_key = u.user_key
                            AND o.stage != 'Closed_Won'
                            AND o.create_date < '2022-04-01'
                            AND o.create_date >= '2022-01-01'
                            Group BY u.b_unit, u.title, u.user_name
                            ORDER BY rev DESC
                            limit 10;"""))
        elif Month_choice == '2022Q2':
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax * o.win_rate/100),0) as rev, u.user_name, u.b_unit,u.title
                            FROM crm_opportunity o, crm_user u
                            WHERE o.user_key = u.user_key
                            AND o.stage != 'Closed_Won'
                            AND o.create_date >= '2022-04-01'
                            AND o.create_date < '2022-07-01'
                            Group BY u.b_unit, u.title, u.user_name
                            ORDER BY rev DESC
                            limit 10;"""))  # insert query here
        else:
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax * o.win_rate/100),0) as rev, u.user_name, u.b_unit,u.title
                            FROM crm_opportunity o, crm_user u
                            WHERE o.user_key = u.user_key
                            AND o.stage != 'Closed_Won'
                            Group BY u.b_unit, u.title, u.user_name
                            ORDER BY rev DESC
                            limit 10;"""))  # insert query here
        # point to existing session and modify
        session["predictive_sales"] = cur.fetchall()  # fetches query and put into object
        session.modified = True
        cur.close()  # closes query
        conn.close()  # closes connection to db
    return render_template('index.html', version=session["db_version"],
                           customer_sales=session.get("customer_sales"),
                           product_sales=session.get("product_sales"),
                           predictive_sales=session.get("predictive_sales"),
                           campaign_roi=session.get("campaign_roi"))

#
@app.route('/campaign_roi', methods=['POST'])
def query_campaign_roi():  # template for some query
    if request.method == 'POST':
        Month_choice = request.form['table-choice']
        conn = get_db_connection()
        cur = conn.cursor()
        if Month_choice == '2021':
            cur.execute(("""SELECT round(SUM(o.deal_amount_aftertax/c.cost),0) as profit,  c.cost, c.campaign_name, c.type
                            FROM crm_opportunity o, crm_campaign c 
                            WHERE o.stage = 'Closed_Won'  
                            AND o.campaign_key = c.campaign_key 
                            AND c.date < '2022-01-01'
                            Group BY c.campaign_name, c.type, c.cost
                            ORDER BY profit DESC
                            limit 10;"""))  # insert query here
        elif Month_choice == '2022':
            cur.execute(("""SELECT round(SUM(o.deal_amount_aftertax/c.cost),0) as profit,  c.cost, c.campaign_name, c.type
                            FROM crm_opportunity o, crm_campaign c 
                            WHERE o.stage = 'Closed_Won'  
                            AND o.campaign_key = c.campaign_key 
                            AND c.date >= '2022-01-01'
                            Group BY c.campaign_name, c.type, c.cost
                            ORDER BY profit DESC
                            limit 10;"""))  # insert query here
        else:
            cur.execute(("""SELECT round(SUM(o.deal_amount_aftertax/c.cost),0) as profit,  c.cost, c.campaign_name, c.type
                            FROM crm_opportunity o, crm_campaign c 
                            WHERE o.stage = 'Closed_Won'  
                            AND o.campaign_key = c.campaign_key 
                            Group BY c.campaign_name, c.type, c.cost
                            ORDER BY profit DESC
                            limit 10;"""))  # insert query here
        # point to existing session and modify
        session["campaign_roi"] = cur.fetchall()  # fetches query and put into object
        session.modified = True
        cur.close()  # closes query
        conn.close()  # closes connection to db
    return render_template('index.html', version=session["db_version"],
                           customer_sales=session.get("customer_sales"),
                           product_sales=session.get("product_sales"),
                           predictive_sales=session.get("predictive_sales"),
                           campaign_roi=session.get("campaign_roi"))

#
if __name__ == '__main__':
    app.run()