import psycopg2
import random
import time
from datetime import date
from faker import Faker

DATABASE_NAME = 'finalproject'
def create_database(dbname):
    """Connect to the PostgreSQL by calling connect_postgres() function
       Create a database named {DATABASE_NAME}
       Close the connection"""
    try:
        # Connect to the default PostgreSQL database
        conn = connect_potsgres()
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Check if the database exists and drop it if it does
        cursor.execute(f"DROP DATABASE IF EXISTS {dbname};")

        # Create a database with the specified name
        cursor.execute(f"CREATE DATABASE {dbname};")

        print(f"Database '{dbname}' created successfully!")

        cursor.close()
        conn.close()
    except Exception as error:
        print(f"Error: {error}")

def connect_potsgres(dbname="postgres"):
    """Connect to the PostgreSQL using psycopg2 with default database
       Return the connection"""
    try:
        # Connection parameters
        user = 'postgres'
        dbname = dbname
        host = 'localhost'
        password = 'hello'
        port = 5430

        # Establish a connection to the PostgreSQL connection
        connection = psycopg2.connect(user = user, dbname=dbname, host=host, password="Super@123", port=5432)

        print ("Connected to the PostgreSQL database successfully!")

        return connection
    except Exception as error:
        print(f"Error: {error}")
        return None



def create_tables(conn):
    try:
        cursor = conn.cursor()

        # Create Users Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Customer (
                customer_id SERIAL PRIMARY KEY,
                customer_name VARCHAR(255),
                customer_email VARCHAR(255),
                customer_shipping_address VARCHAR(255),
                customer_region VARCHAR(255)
            );
        """)
        # Create Payments Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Payments (
                payment_id SERIAL PRIMARY KEY,
                payment_date DATE,
                payment_mode VARCHAR(255)
            );
        """)        
        # Create Orders Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Orders (
                order_id INT UNIQUE ,
                customer_id INT,
                order_date DATE,
                payment_id INT,
                quantity INT,
                price INT,
                order_sequence_id bigint,
                PRIMARY KEY(order_id,order_sequence_id),
                FOREIGN KEY (customer_id) REFERENCES Customer (customer_id),
                FOREIGN KEY (payment_id) REFERENCES Payments (payment_id)
            );
        """)

        # New Orders Table
        cursor.execute("""
            DROP TABLE IF EXISTS Orders;

            CREATE TABLE IF NOT EXISTS Orders (
                order_id INT UNIQUE,
                customer_id INT,
                order_date DATE,
                payment_id INT,
                quantity INT,
                price INT,
                order_sequence_id bigint,
                PRIMARY KEY (order_id, order_sequence_id),
                UNIQUE (order_sequence_id), -- Add a unique constraint here
                FOREIGN KEY (customer_id) REFERENCES Customer (customer_id),
                FOREIGN KEY (payment_id) REFERENCES Payments (payment_id)
            );
        """)


        # Create Products Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Products (
                product_id SERIAL UNIQUE PRIMARY KEY,
                product_name VARCHAR(255),
                product_price VARCHAR(255),
                product_categories VARCHAR(255)
            );
        """)





        # Create OrderItems Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS OrderItems (
                orderitem_id SERIAL PRIMARY KEY,
                order_sequence_id bigint,
                product_id INT REFERENCES Products (product_id),
                FOREIGN KEY (order_sequence_id) REFERENCES Orders (order_sequence_id)
            );
        """)

        # Create Shipping Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Shipments (
                shipping_id SERIAL PRIMARY KEY,
                order_id INT,
                shipment_date DATE,
                customer_shipping_address VARCHAR(255),
                customer_region VARCHAR(255),
                FOREIGN KEY (order_id) REFERENCES Orders (order_id)
            );
        """)

        conn.commit()
        cursor.close()
        print("Tables created successfully.")
    except Exception as error:
        print(f"Error creating tables: {error}")


def vertical_partitioning(conn):

    cursor = conn.cursor()

    # cursor.execute(f"CREATE TABLE IF NOT EXISTS {SHIPMENTS_TABLE} (shipment_id serial, order_id int, shipment_date date, customer_region text, customer_shipping_address text);")

    cursor.execute(f"CREATE TABLE SHIPMENT_DETAILS_TABLE AS SELECT shipping_id, order_id, shipment_date FROM Shipments;")
    cursor.execute(f"CREATE TABLE CUSTOMER_DETAILS_TABLE AS SELECT shipping_id, customer_region, customer_shipping_address FROM Shipments;")
    print (" vertical partitioning done successfully!")
    conn.commit()
    cursor.close()


# 

def horizontal_partitioning(conn):
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS Products_10_to_100;")
    cursor.execute("DROP TABLE IF EXISTS Products_101_to_250;")
    cursor.execute("DROP TABLE IF EXISTS Products_251_to_500;")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Products_10_to_100 (
            CHECK (product_price::numeric >= 10 AND product_price::numeric <= 100)
        ) INHERITS (Products)
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Products_101_to_250 (
            CHECK (product_price::numeric > 100 AND product_price::numeric <= 250)
        ) INHERITS (Products)
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Products_251_to_500 (
            CHECK (product_price::numeric > 250 AND product_price::numeric <= 500)
        ) INHERITS (Products)
    """)
    
    # Create trigger function to redirect data
    cursor.execute("""
        CREATE OR REPLACE FUNCTION products_insert_trigger()
        RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.product_price::numeric >= 10 AND NEW.product_price::numeric <= 100 THEN
                INSERT INTO Products_10_to_100 VALUES (NEW.*);
            ELSIF NEW.product_price::numeric > 100 AND NEW.product_price::numeric <= 250 THEN
                INSERT INTO Products_101_to_250 VALUES (NEW.*);
            ELSIF NEW.product_price::numeric > 250 AND NEW.product_price::numeric <= 500 THEN
                INSERT INTO Products_251_to_500 VALUES (NEW.*);
            ELSE
                RAISE EXCEPTION 'Price out of range';
            END IF;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
    """)
    
    cursor.execute("DROP TRIGGER IF EXISTS products_insert_trigger ON Products;")


    # Create the trigger on the "Products" table
    cursor.execute("""
        CREATE TRIGGER products_insert_trigger
        BEFORE INSERT ON Products
        FOR EACH ROW
        EXECUTE FUNCTION products_insert_trigger();
    """)
    print (" Horizontal partitioning done successfully!")
    conn.commit()
    cursor.close()




# Define a function to generate random data
def generate_random_data():
    fake = Faker()
    customer_name = fake.name()
    customer_email = fake.email()
    customer_shipping_address = fake.address()
    customer_region = random.choice(["LATAM", "EMEA", "APJ"])
    payment_date = fake.date_of_birth(minimum_age=18, maximum_age=65)
    payment_mode = random.choice(["Credit Card", "PayPal", "Cash"])
    order_date = fake.date_between(start_date='-30d', end_date='today') 
    quantity = random.randint(1, 10)
    price = random.randint(10, 500)
    orderseq  = str(random.randint(1, 100000)) + str(int(time.time()))
    # product_name = fake.word()
    product_categories = random.choice([
        "Electronics",
        "Clothing and Fashion",
        "Home and Furniture"
    ])



    # Set product name based on the chosen category
    if product_categories == "Electronics":
        product_name = fake.random_element(elements=("Smartphone", "Laptop", "Camera"))
    elif product_categories == "Clothing and Fashion":
        product_name = fake.random_element(elements=("Shirt", "Dress", "Shoes"))
    elif product_categories == "Home and Furniture":
        product_name = fake.random_element(elements=("Sofa", "Table", "Bedding"))


    # Generate a random price
    product_price = random.randint(10, 500)
    shipment_date = fake.date_between(start_date=order_date, end_date='today')
    customer_shipping_address = fake.address()
    return (customer_name, customer_email, customer_shipping_address, customer_region,
            payment_date, payment_mode, order_date, quantity, price,
            product_name, product_price, product_categories, shipment_date,
            customer_shipping_address, customer_region,orderseq)



def insert_random_data(conn, num_records):
    cursor = conn.cursor()

    for i in range(num_records):
        data = generate_random_data()

        cursor.execute("""
            INSERT INTO Customer (customer_name, customer_email, customer_shipping_address, customer_region)
            VALUES (%s, %s, %s, %s)
        """, (data[0], data[1], data[2], data[3]))

        cursor.execute("""
            INSERT INTO Payments (payment_date, payment_mode)
            VALUES (%s, %s)
        """, (data[4], data[5]))

        cursor.execute("""
            INSERT INTO Orders (order_id, customer_id, order_date, payment_id, quantity, order_sequence_id)
            VALUES (%s, (SELECT customer_id FROM Customer ORDER BY random() LIMIT 1), %s, (SELECT payment_id FROM Payments ORDER BY random() LIMIT 1), %s, %s)
        """, (i, data[6], data[7], data[15]))

        cursor.execute("""
            INSERT INTO Products (product_name, product_price, product_categories)
            VALUES (%s, %s, %s)
        """, (data[9], data[10], data[11]))

        cursor.execute("""
            INSERT INTO Shipments (order_id, shipment_date, customer_shipping_address, customer_region)
            VALUES (%s, %s, %s, %s)
        """, (i, data[12], data[13], data[14]))

        # Route the data to the appropriate partitioned table based on product_price
        if 10 <= data[10] <= 100:
            cursor.execute("INSERT INTO Products_10_to_100 (product_name, product_price, product_categories) VALUES (%s, %s, %s)",
                           (data[9], data[10], data[11]))
        elif 101 <= data[10] <= 250:
            cursor.execute("INSERT INTO Products_101_to_250 (product_name, product_price, product_categories) VALUES (%s, %s, %s)",
                           (data[9], data[10], data[11]))
        elif 251 <= data[10] <= 500:
            cursor.execute("INSERT INTO Products_251_to_500 (product_name, product_price, product_categories) VALUES (%s, %s, %s)",
                           (data[9], data[10], data[11]))
        else:
            # Handle data that doesn't fit into any partition
            print("Data out of range: ", data[10])

    conn.commit()
    cursor.close()
    print(f"Inserted {num_records} rows of data into the tables.")




def create_replicated_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Customer_Region_LATAM (
            LIKE Customer INCLUDING ALL,
            CHECK (customer_region = 'LATAM')
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Customer_Region_EMEA (
            LIKE Customer INCLUDING ALL,
            CHECK (customer_region = 'EMEA')
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Customer_Region_APJ (
            LIKE Customer INCLUDING ALL,
            CHECK (customer_region = 'APJ')
        );
    """)

    conn.commit()
    cursor.close()


def create_replication_trigger(conn):
    cursor = conn.cursor()

    # Trigger function for replication
    cursor.execute("""
        CREATE OR REPLACE FUNCTION replicate_customer_trigger()
        RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.customer_region = 'LATAM' THEN
                INSERT INTO Customer_Region_LATAM VALUES (NEW.*);
            ELSIF NEW.customer_region = 'EMEA' THEN
                INSERT INTO Customer_Region_EMEA VALUES (NEW.*);
            ELSIF NEW.customer_region = 'APJ' THEN
                INSERT INTO Customer_Region_APJ VALUES (NEW.*);
            END IF;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
    """)

    cursor.execute("DROP TRIGGER IF EXISTS replicate_customer_trigger ON Customer;")
    
    # Trigger to call the replication function
    cursor.execute("""
        CREATE TRIGGER replicate_customer_trigger
        BEFORE INSERT ON Customer
        FOR EACH ROW
        EXECUTE FUNCTION replicate_customer_trigger();
    """)

    conn.commit()
    cursor.close()
    print (" Replaction  done successfully!")

def IndexCreation(conn):
    cursor = conn.cursor()

    cursor.execute("""
CREATE INDEX customer_name_apj ON customer_region_apj(customer_name)
    """)

    # Trigger function for replication
    cursor.execute("""
CREATE INDEX customer_name_emea ON customer_region_emea(customer_name)
    """)

    cursor.execute("""
CREATE INDEX customer_name_latam ON customer_region_latam(customer_name)
    """)

    cursor.execute("""
CREATE INDEX customer_name_ ON customer(customer_name)
    """)
    print (" Index Creation  done successfully!")
def retrieve_data(conn):
    try:
        cursor = conn.cursor()

        # Query 1: Retrieve top 3 customers based on the number of orders placed
        cursor.execute("""
            SELECT
                c.customer_name,
                COUNT(o.order_id) AS total_orders
            FROM
                Customer c
                JOIN Orders o ON c.customer_id = o.customer_id
            GROUP BY
                c.customer_name
            ORDER BY
                total_orders DESC
            LIMIT 3;
        """)
        result1 = cursor.fetchall()

        print("\nQuery 1: Top 3 Customers based on Total Orders")
        print("-----------------------------------------------------------------")
        for row in result1:
            print(f"{row[0]}: {row[1]} orders")

        #  Query 2: Retrieve product categories and the average price
        cursor.execute("""
            SELECT
                p.product_categories,
                AVG(p.product_price::numeric) AS average_price
            FROM
                Products p
            GROUP BY
                p.product_categories
            ORDER BY
                average_price DESC;
        """)
        result2 = cursor.fetchall()

        print("\ Query 2: Product Categories and Average Price")
        print("----------------------------------------------------")
        for row in result2:
            print(f"{row[0]}: ${row[1]:.2f}")

        cursor.close()

    except Exception as error:
        print(f"Error retrieving data: {error}")
    finally:
        
        cursor.close()


if __name__ == '__main__':

    create_database(DATABASE_NAME)
    with connect_potsgres(dbname=DATABASE_NAME) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        num_records = 100 
        create_tables(conn)
        vertical_partitioning(conn)
        horizontal_partitioning(conn)
        insert_random_data(conn, num_records) 
        create_replicated_tables(conn)
        create_replication_trigger(conn)      
        IndexCreation(conn)
        retrieve_data(conn)
        print('Done')
