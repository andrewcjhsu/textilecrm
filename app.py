import os
import psycopg2
import jinja2
from flask import Flask, redirect, request, render_template, url_for, session, flash
from urllib.parse import urlparse

app = Flask(__name__)
app.secret_key = 'I Love Databases'


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
    # Sales and Attendance by Day of Week Section Display
    cur.execute("""SELECT sum (o.deal_amount_aftertax) as rev, p.product_name, p.category, p.description
                    FROM crm_opportunity o, crm_product p  
                    WHERE o.stage = 'Closed_Won'  
                    AND o.product_key = p.product_key 
                    GROUP BY p.category, p.product_name, p.description
                    ORDER BY rev DESC
                    limit 10;""")
    session["sales_dow"] = cur.fetchall()

    # Sales and Attendance by Film Section Display
    cur.execute("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, p.product_name, p.category, p.description
                    FROM crm_opportunity o, crm_product p  
                    WHERE o.stage = 'Closed_Won'  
                    AND o.product_key = p.product_key 
                    GROUP BY p.category, p.product_name, p.description
                    ORDER BY rev desc
                    limit 10;""")
    session["sales_by_film"] = cur.fetchall()

    # # Return on Investment By Film Section Display
    # cur.execute("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, u.user_name, u.b_unit,u.title
    #                 FROM crm_opportunity o, crm_user u
    #                 WHERE o.user_key = u.user_key
    #                 AND o.stage != 'Closed_Won'
    #                 Group BY u.b_unit, u.title, u.user_name
    #                 ORDER BY rev DESC;""")
    # session["film_roi"] = cur.fetchall()

    # Return on Investment By Film Section Display
    cur.execute("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, u.user_name, u.b_unit,u.title
                        FROM crm_opportunity o, crm_user u  
                        WHERE o.user_key = u.user_key 
                        AND o.stage != 'Closed_Won'
                        Group BY u.b_unit, u.title, u.user_name
                        ORDER BY rev DESC;""")
    session["promo_roi"] = cur.fetchall()
#
#     # Most popular promotion by member’s age and gender
#     cur.execute("""SELECT m1.age, m1.gender,p1.planner,p1.promotion_name, count(*) total_redemption
#                             FROM transaction_fact t1, date_dimension d1, promotion_dimension p1, member_dimension m1
#                             WHERE t1.date_key = d1.date_key
#                             AND t1.promotion_key = p1.promotion_key
#                             AND t1.member_key = m1.member_key
#                             GROUP BY p1.planner, p1.promotion_name, m1.age, m1.gender
#                             HAVING count(*) >=ALL(
#                                 SELECT count(*)
#                                 FROM transaction_fact t2, date_dimension d2, promotion_dimension p2, member_dimension m2
#                                 WHERE t2.date_key = d2.date_key
#                                 AND t2.promotion_key = p2.promotion_key
#                                 AND t2.member_key = m2.member_key
#                                 AND m1.age = m2.age
#                                 AND m1.gender = m2.gender
#                                 GROUP BY p2.planner, p2.promotion_name, m2.age, m2.gender)
#                             ORDER BY m1.age, m1.gender DESC;""")
#     session["pop_promo"] = cur.fetchall()
#
    # close the communication with the PostgreSQL
    cur.close()
    return render_template('index.html', version=session["db_version"],
                           sales_dow=session.get("sales_dow"),
                           sales_by_film=session.get("sales_by_film"),
                           # film_roi=session.get("film_roi"))
                           promo_roi=session.get("promo_roi"))
                           # pop_promo=session.get("pop_promo"))
#
#
# # def index():
# #     conn = get_db_connection()
# #     cur = conn.cursor()
# #     cur.execute('SELECT * FROM membership_dimension;')
# #     members = cur.fetchall()
# #     cur.close()
# #     conn.close()
# #     return render_template('index.html', members=members)
#
#
@app.route('/sales_dow', methods=['POST'])
def query_sales_dow():  # template for some query
    if request.method == 'POST':
        Month_choice = request.form.get('table-choice')
        print(Month_choice)
        conn = get_db_connection()
        cur = conn.cursor()
        if Month_choice == '2021':
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, c.customer_name, c.type, c.class
                            FROM crm_opportunity o, crm_customer c  
                            WHERE o.stage = 'Closed_Won' 
                            AND o.customer_key = c.customer_key 
                            AND o.close_date < '2022-01-01'
                            GROUP BY c.type, c.class, c.customer_name
                            ORDER BY rev DESC
                            limit 10;"""))  # insert query here
        elif Month_choice == '2022':
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, c.customer_name, c.type, c.class
                            FROM crm_opportunity o, crm_customer c  
                            WHERE o.stage = 'Closed_Won' 
                            AND o.customer_key = c.customer_key 
                            AND o.close_date >= '2022-01-01'
                            GROUP BY c.type, c.class, c.customer_name
                            ORDER BY rev DESC
                            limit 10; """))  # insert query here
        else:
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, c.customer_name, c.type, c.class
                            FROM crm_opportunity o, crm_customer c  
                            WHERE o.stage = 'Closed_Won' 
                            AND o.customer_key = c.customer_key 
                            GROUP BY c.type, c.class, c.customer_name
                            ORDER BY rev DESC
                            limit 10; """))  # insert query here
        # point to existing session and modify
        session["sales_dow"] = cur.fetchall()  # fetches query and put into object
        session.modified = True

        cur.close()  # closes query
        conn.close()  # closes connection to db
    return render_template('index.html', version=session["db_version"],
                           sales_dow=session.get("sales_dow"),
                           sales_by_film=session.get("sales_by_film"),
                           promo_roi=session.get("promo_roi"))


@app.route('/sales_by_film', methods=['POST'])
def query_sales_by_film():  # template for some query
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
                            ORDER BY rev desc;"""))  # insert query here
        elif Month_choice == '2022':
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, p.product_name, p.category, p.description
                            FROM crm_opportunity o, crm_product p  
                            WHERE o.stage = 'Closed_Won' 
                            AND o.close_date >= '2022-01-01'
                            AND o.product_key = p.product_key 
                            GROUP BY p.category, p.product_name, p.description
                            ORDER BY rev desc;"""))  # insert query here
        else:
            cur.execute(("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, p.product_name, p.category, p.description
                            FROM crm_opportunity o, crm_product p  
                            WHERE o.stage = 'Closed_Won'  
                            AND o.product_key = p.product_key 
                            GROUP BY p.category, p.product_name, p.description
                            ORDER BY rev desc
                            limit 10;"""))  # insert query here
        # point to existing session and modify
        session["sales_by_film"] = cur.fetchall()  # fetches query and put into object
        session.modified = True
        cur.close()  # closes query
        conn.close()  # closes connection to db
    return render_template('index.html', version=session["db_version"],
                           sales_dow=session.get("sales_dow"),
                           sales_by_film=session.get("sales_by_film"),
                           promo_roi=session.get("promo_roi"))


# @app.route('/promo_roi', methods=['POST'])
# def query_promo_roi():  # template for some query
#     if request.method == 'POST':
#         Month_choice = request.form['table-choice']
#         conn = get_db_connection()
#         cur = conn.cursor()
#         if Month_choice == '2021':
#             cur.execute(("""SELECT round(sum (o.deal_amount_aftertax * o.win_rate/100),0) as rev, u.user_name, u.b_unit,u.title
#                             FROM crm_opportunity o, crm_user u
#                             WHERE o.user_key = u.user_key
#                             AND o.stage != 'Closed_Won'
#                             AND o.create_date < '2022-04-01'
#                             Group BY u.b_unit, u.title, u.user_name
#                             ORDER BY rev DESC;"""))  # insert query here
#         elif Month_choice == '2022Q1':
#             cur.execute(("""SELECT round(sum (o.deal_amount_aftertax * o.win_rate/100),0) as rev, u.user_name, u.b_unit,u.title
#                             FROM crm_opportunity o, crm_user u
#                             WHERE o.user_key = u.user_key
#                             AND o.stage != 'Closed_Won'
#                             AND o.create_date < '2022-04-01'
#                             AND o.create_date >= '2022-01-01'
#                             Group BY u.b_unit, u.title, u.user_name
#                             ORDER BY rev DESC;"""))
#         elif Month_choice == '2022Q2':
#             cur.execute(("""SELECT round(sum (o.deal_amount_aftertax * o.win_rate/100),0) as rev, u.user_name, u.b_unit,u.title
#                             FROM crm_opportunity o, crm_user u
#                             WHERE o.user_key = u.user_key
#                             AND o.stage != 'Closed_Won'
#                             AND o.create_date >= '2022-04-01'
#                             AND o.create_date < '2022-07-01'
#                             Group BY u.b_unit, u.title, u.user_name
#                             ORDER BY rev DESC;"""))  # insert query here
#         else:
#             cur.execute(("""SELECT round(sum (o.deal_amount_aftertax),0) as rev, u.user_name, u.b_unit,u.title
#                             FROM crm_opportunity o, crm_user u
#                             WHERE o.user_key = u.user_key
#                             AND o.stage != 'Closed_Won'
#                             Group BY u.b_unit, u.title, u.user_name
#                             ORDER BY rev DESC;"""))  # insert query here
#         # point to existing session and modify
#         session["promo_roi"] = cur.fetchall()  # fetches query and put into object
#         session.modified = True
#         cur.close()  # closes query
#         conn.close()  # closes connection to db
#     return render_template('index.html', version=session["db_version"],
#                            sales_dow=session.get("sales_dow"),
#                            sales_by_film=session.get("sales_by_film"),
#                            # film_roi=session.get("film_roi"),
#                            promo_roi=session.get("promo_roi"))
#                            # pop_promo=session.get("pop_promo"))

#
# @app.route('/pop_promo', methods=['POST'])
# def query_pop_promo():  # template for some query
#     if request.method == 'POST':
#         Month_choice = request.form['table-choice']
#         conn = get_db_connection()
#         cur = conn.cursor()
#         if Month_choice == 'September':
#             cur.execute(("""SELECT m1.age, m1.gender,p1.planner,p1.promotion_name, count(*) total_redemption
#                             FROM transaction_fact t1, date_dimension d1, promotion_dimension p1, member_dimension m1
#                             WHERE t1.date_key = d1.date_key
#                             AND extract(month from d1.date) = 9
#                             AND t1.promotion_key = p1.promotion_key
#                             AND t1.member_key = m1.member_key
#                             GROUP BY p1.planner, p1.promotion_name, m1.age, m1.gender
#                             HAVING count(*) >=ALL(
#                                 SELECT count(*)
#                                 FROM transaction_fact t2, date_dimension d2, promotion_dimension p2, member_dimension m2
#                                 WHERE t2.date_key = d2.date_key
#                                 AND extract(month from d2.date) = 9
#                                 AND t2.promotion_key = p2.promotion_key
#                                 AND t2.member_key = m2.member_key
#                                 AND m1.age = m2.age
#                                 AND m1.gender = m2.gender
#                                 GROUP BY p2.planner, p2.promotion_name, m2.age, m2.gender)
#                             ORDER BY m1.age, m1.gender DESC;"""))  # insert query here
#         elif Month_choice == 'October':
#             cur.execute(("""SELECT m1.age, m1.gender,p1.planner,p1.promotion_name, count(*) total_redemption
#                             FROM transaction_fact t1, date_dimension d1, promotion_dimension p1, member_dimension m1
#                             WHERE t1.date_key = d1.date_key
#                             AND extract(month from d1.date) = 10
#                             AND t1.promotion_key = p1.promotion_key
#                             AND t1.member_key = m1.member_key
#                             GROUP BY p1.planner, p1.promotion_name, m1.age, m1.gender
#                             HAVING count(*) >=ALL(
#                                 SELECT count(*)
#                                 FROM transaction_fact t2, date_dimension d2, promotion_dimension p2, member_dimension m2
#                                 WHERE t2.date_key = d2.date_key
#                                 AND extract(month from d2.date) = 10
#                                 AND t2.promotion_key = p2.promotion_key
#                                 AND t2.member_key = m2.member_key
#                                 AND m1.age = m2.age
#                                 AND m1.gender = m2.gender
#                                 GROUP BY p2.planner, p2.promotion_name, m2.age, m2.gender)
#                             ORDER BY m1.age, m1.gender DESC;"""))  # insert query here
#         else:
#             cur.execute(("""SELECT m1.age, m1.gender,p1.planner,p1.promotion_name, count(*) total_redemption
#                             FROM transaction_fact t1, date_dimension d1, promotion_dimension p1, member_dimension m1
#                             WHERE t1.date_key = d1.date_key
#                             AND t1.promotion_key = p1.promotion_key
#                             AND t1.member_key = m1.member_key
#                             GROUP BY p1.planner, p1.promotion_name, m1.age, m1.gender
#                             HAVING count(*) >=ALL(
#                                 SELECT count(*)
#                                 FROM transaction_fact t2, date_dimension d2, promotion_dimension p2, member_dimension m2
#                                 WHERE t2.date_key = d2.date_key
#                                 AND t2.promotion_key = p2.promotion_key
#                                 AND t2.member_key = m2.member_key
#                                 AND m1.age = m2.age
#                                 AND m1.gender = m2.gender
#                                 GROUP BY p2.planner, p2.promotion_name, m2.age, m2.gender)
#                             ORDER BY m1.age, m1.gender DESC;"""))  # insert query here
#         # point to existing session and modify
#         session["pop_promo"] = cur.fetchall()  # fetches query and put into object
#         session.modified = True
#         cur.close()  # closes query
#         conn.close()  # closes connection to db
#     return render_template('index.html', version=session["db_version"],
#                            sales_dow=session.get("sales_dow"),
#                            sales_by_film=session.get("sales_by_film"),
#                            film_roi=session.get("film_roi"),
#                            promo_roi=session.get("promo_roi"),
#                            pop_promo=session.get("pop_promo"))
#
#
if __name__ == '__main__':
    app.run()