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
    cur.execute("""	SELECT p.category, sum (o.deal_amount_aftertax) as rev 
	                FROM crm_opportunity o, crm_product p  
	                WHERE o.stage = 'Closed_Won'  
                    AND o.product_key = p.product_key 
                    GROUP BY p.category 
                    ORDER BY rev DESC; """)
    session["sales_dow"] = cur.fetchall()

#     # Sales and Attendance by Film Section Display
#     cur.execute("""SELECT f.film_name, f.genre, sum(t.total_amount_paid) total_sales, sum(t.adult_amount_paid) adult_sales,
#                     sum(t.kid_amount_paid) kid_sales, sum(t.adult_seat + t.kid_seat) total_attendance, sum(t.adult_seat) adults_attendance,
#                     sum(t.kid_seat) kid_attendance FROM transaction_fact t, date_dimension d, film_dimension f
#                     WHERE t.date_key = d.date_key AND t.film_key = f.film_key
#                          GROUP BY f.film_name, f.genre ORDER BY total_sales DESC;""")
#     session["sales_by_film"] = cur.fetchall()
#
#     # Return on Investment By Film Section Display
#     cur.execute("""SELECT f.film_name, f.genre, sum(t.total_amount_paid) total_sales, max(f.procurement_cost) cost, (sum(t.total_amount_paid) - max(f.procurement_cost)) profit, ((sum(t.total_amount_paid) - max(f.procurement_cost))/max(f.procurement_cost)) ROI
#                      FROM transaction_fact t, film_dimension f
#                      WHERE t.film_key = f.film_key
#                      GROUP BY f.film_name, f.genre
#                      ORDER BY roi DESC;""")
#     session["film_roi"] = cur.fetchall()
#
#     # Return on Investment By Film Section Display
#     cur.execute("""SELECT p.planner, p.promotion_name, SUM(t.total_amount_paid) total_sales_from_promo, (MAX(p.cost_per_member) * count(*)) promo_costs, (SUM(t.total_amount_paid)-(MAX(p.cost_per_member) * count(*))) profit, (SUM(t.total_amount_paid)-(MAX(p.cost_per_member) * count(*)))/(MAX(p.cost_per_member) * count(*)) promo_roi
#                          FROM transaction_fact t, date_dimension d, promotion_dimension p
#                          WHERE t.date_key = d.date_key AND t.promotion_key = p.promotion_key
#                          GROUP BY p.planner, p.promotion_name
#                          ORDER BY promo_roi DESC;""")
#     session["promo_roi"] = cur.fetchall()
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
                           sales_dow=session.get("sales_dow"))
                           # sales_by_film=session.get("sales_by_film"),
                           # film_roi=session.get("film_roi"),
                           # promo_roi=session.get("promo_roi"),
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
# @app.route('/sales_dow', methods=['POST'])
# def query_sales_dow():  # template for some query
#     if request.method == 'POST':
#         Month_choice = request.form.get('table-choice')
#         print(Month_choice)
#         conn = get_db_connection()
#         cur = conn.cursor()
#         if Month_choice == 'September':
#             cur.execute(("""SELECT d.weekday, sum(t.total_amount_paid) total_sales, sum(t.adult_amount_paid) adult_sales, sum(t.kid_amount_paid) kid_sales, sum(t.adult_seat + t.kid_seat) total_attendance, sum(t.adult_seat) adults_attendance, sum(t.kid_seat) kid_attendance
#             FROM transaction_fact t, date_dimension d
#             WHERE t.date_key = d.date_key and extract(month from d.date) = 9
#             GROUP BY d.weekday
#             ORDER BY total_sales DESC;"""))  # insert query here
#         elif Month_choice == 'October':
#             cur.execute(("""SELECT d.weekday, sum(t.total_amount_paid) total_sales, sum(t.adult_amount_paid) adult_sales, sum(t.kid_amount_paid) kid_sales, sum(t.adult_seat + t.kid_seat) total_attendance, sum(t.adult_seat) adults_attendance, sum(t.kid_seat) kid_attendance
#             FROM transaction_fact t, date_dimension d
#             WHERE t.date_key = d.date_key and extract(month from d.date) = 10
#             GROUP BY d.weekday
#             ORDER BY total_sales DESC;"""))  # insert query here
#         else:
#             cur.execute(("""SELECT d.weekday, sum(t.total_amount_paid) total_sales, sum(t.adult_amount_paid) adult_sales, sum(t.kid_amount_paid) kid_sales, sum(t.adult_seat + t.kid_seat) total_attendance, sum(t.adult_seat) adults_attendance, sum(t.kid_seat) kid_attendance
#             FROM transaction_fact t, date_dimension d
#             WHERE t.date_key = d.date_key
#             GROUP BY d.weekday
#             ORDER BY total_sales DESC;"""))  # insert query here
#         # point to existing session and modify
#         session["sales_dow"] = cur.fetchall()  # fetches query and put into object
#         session.modified = True
#
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
# @app.route('/sales_by_film', methods=['POST'])
# def query_sales_by_film():  # template for some query
#     if request.method == 'POST':
#         Month_choice = request.form['table-choice']
#         conn = get_db_connection()
#         cur = conn.cursor()
#         if Month_choice == 'September':
#             cur.execute(("""
#                          SELECT f.film_name, f.genre, sum(t.total_amount_paid) total_sales, sum(t.adult_amount_paid) adult_sales,
#                          sum(t.kid_amount_paid) kid_sales, sum(t.adult_seat + t.kid_seat) total_attendance, sum(t.adult_seat) adults_attendance,
#                          sum(t.kid_seat) kid_attendance FROM transaction_fact t, date_dimension d, film_dimension f
#                          WHERE t.date_key = d.date_key AND extract(month from d.date) = 9 AND t.film_key = f.film_key
#                          GROUP BY f.film_name, f.genre ORDER BY total_sales DESC;"""))  # insert query here
#         elif Month_choice == 'October':
#             cur.execute(("""SELECT f.film_name, f.genre, sum(t.total_amount_paid) total_sales, sum(t.adult_amount_paid) adult_sales,
#                          sum(t.kid_amount_paid) kid_sales, sum(t.adult_seat + t.kid_seat) total_attendance,
#                          sum(t.adult_seat) adults_attendance, sum(t.kid_seat) kid_attendance
#                          FROM transaction_fact t, date_dimension d, film_dimension f WHERE t.date_key = d.date_key
#                          AND extract(month from d.date) = 10 AND t.film_key = f.film_key GROUP BY f.film_name, f.genre
#                          ORDER BY total_sales DESC;"""))  # insert query here
#         else:
#             cur.execute(("""SELECT f.film_name, f.genre, sum(t.total_amount_paid) total_sales, sum(t.adult_amount_paid) adult_sales,
#                          sum(t.kid_amount_paid) kid_sales, sum(t.adult_seat + t.kid_seat) total_attendance,
#                          sum(t.adult_seat) adults_attendance, sum(t.kid_seat) kid_attendance
#                          FROM transaction_fact t, date_dimension d, film_dimension f WHERE t.date_key = d.date_key
#                          AND t.film_key = f.film_key GROUP BY f.film_name, f.genre ORDER BY total_sales DESC;"""))  # insert query here
#         # point to existing session and modify
#         session["sales_by_film"] = cur.fetchall()  # fetches query and put into object
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
# @app.route('/promo_roi', methods=['POST'])
# def query_promo_roi():  # template for some query
#     if request.method == 'POST':
#         Month_choice = request.form['table-choice']
#         conn = get_db_connection()
#         cur = conn.cursor()
#         if Month_choice == 'September':
#             cur.execute(("""SELECT p.planner, p.promotion_name, SUM(t.total_amount_paid) total_sales_from_promo, (MAX(p.cost_per_member) * count(*)) promo_costs, (SUM(t.total_amount_paid)-(MAX(p.cost_per_member) * count(*))) profit, (SUM(t.total_amount_paid)-(MAX(p.cost_per_member) * count(*)))/(MAX(p.cost_per_member) * count(*)) promo_roi
#                          FROM transaction_fact t, date_dimension d, promotion_dimension p
#                          WHERE t.date_key = d.date_key AND extract(month from d.date) = 9 AND t.promotion_key = p.promotion_key
#                          GROUP BY p.planner, p.promotion_name
#                          ORDER BY promo_roi DESC;"""))  # insert query here
#         elif Month_choice == 'October':
#             cur.execute(("""SELECT p.planner, p.promotion_name, SUM(t.total_amount_paid) total_sales_from_promo, (MAX(p.cost_per_member) * count(*)) promo_costs, (SUM(t.total_amount_paid)-(MAX(p.cost_per_member) * count(*))) profit, (SUM(t.total_amount_paid)-(MAX(p.cost_per_member) * count(*)))/(MAX(p.cost_per_member) * count(*)) promo_roi
#                          FROM transaction_fact t, date_dimension d, promotion_dimension p
#                          WHERE t.date_key = d.date_key AND extract(month from d.date) = 10 AND t.promotion_key = p.promotion_key
#                          GROUP BY p.planner, p.promotion_name
#                          ORDER BY promo_roi DESC;"""))  # insert query here
#         else:
#             cur.execute(("""SELECT p.planner, p.promotion_name, SUM(t.total_amount_paid) total_sales_from_promo, (MAX(p.cost_per_member) * count(*)) promo_costs, (SUM(t.total_amount_paid)-(MAX(p.cost_per_member) * count(*))) profit, (SUM(t.total_amount_paid)-(MAX(p.cost_per_member) * count(*)))/(MAX(p.cost_per_member) * count(*)) promo_roi
#                          FROM transaction_fact t, date_dimension d, promotion_dimension p
#                          WHERE t.date_key = d.date_key AND t.promotion_key = p.promotion_key
#                          GROUP BY p.planner, p.promotion_name
#                          ORDER BY promo_roi DESC;"""))  # insert query here
#         # point to existing session and modify
#         session["promo_roi"] = cur.fetchall()  # fetches query and put into object
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