create database aircargo;
use air_cargo;

-- to create route_details table using suitable data types for the fields
CREATE TABLE route_details (
  route_id INT NOT NULL UNIQUE,
  flight_num VARCHAR(10) NOT NULL CHECK (flight_num LIKE '___-____'),
  origin_airport VARCHAR(50) NOT NULL,
  destination_airport VARCHAR(50) NOT NULL,
  aircraft_id INT NOT NULL,
  distance_miles DECIMAL(10, 2) NOT NULL CHECK (distance_miles > 0),
  PRIMARY KEY (route_id)
);

-- to display all the passengers (customers) who have travelled in routes 01 to 25
SELECT DISTINCT c.first_name, c.last_name 
FROM passengers_on_flights p 
JOIN customer c ON p.customer_id = c.customer_id 
WHERE p.route_id BETWEEN '01' AND '25';

-- to identify the number of passengers and total revenue in business class from the ticket_details table
SELECT SUM(no_of_tickets) AS total_passengers, SUM(Price_per_ticket * no_of_tickets) AS total_revenue 
FROM ticket_details 
WHERE class_id = 'Bussiness';

--  to display the full name of the customer by extracting the first name and last name from the customer table
SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM customer;

--  to extract the customers who have registered and booked a ticket. Use data from the customer and ticket_details tables
SELECT c.customer_id, c.first_name, c.last_name
FROM customer c
INNER JOIN ticket_details t
ON c.customer_id = t.customer_id;

-- to identify the customer’s first name and last name based on their customer ID and brand (Emirates) from the ticket_details table
SELECT c.first_name, c.last_name
FROM customer c
JOIN ticket_details t ON c.customer_id = t.customer_id
WHERE t.brand = 'Emirates';

--  to identify the customers who have travelled by Economy Plus class using Group By and Having clause on the passengers_on_flights table
SELECT p.customer_id, c.first_name, c.last_name
FROM passengers_on_flights p
INNER JOIN customer c ON p.customer_id = c.customer_id
WHERE p.class_id = 'Economy Plus'
GROUP BY p.customer_id, c.first_name, c.last_name

--  to identify whether the revenue has crossed 10000 using the IF clause on the ticket_details table
SELECT IF(SUM(Price_per_ticket * no_of_tickets) > 10000, 'Revenue crossed 10000', 'Revenue is below 10000') AS revenue_status
FROM ticket_details;

-- to create and grant access to a new user to perform operations on a database
CREATE USER 'newuser'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON air_cargo.* TO 'newuser'@'localhost';
FLUSH PRIVILEGES;

-- to find the maximum ticket price for each class using window functions on the ticket_details table
SELECT distinct class_id, MAX(price_per_ticket) OVER (PARTITION BY class_id) AS max_price
FROM ticket_details;

-- to extract the passengers whose route ID is 4 by improving the speed and performance of the passengers_on_flights table
CREATE INDEX idx_passengers_route_id ON passengers_on_flights (route_id);
SELECT * FROM passengers_on_flights WHERE route_id = 4;

-- to view the execution plan of the passengers_on_flights table for route id 4
EXPLAIN SELECT * FROM passengers_on_flights WHERE route_id = 4;

-- to calculate the total price of all tickets booked by a customer across different aircraft IDs using rollup function
SELECT customer_id, aircraft_id, SUM(price_per_ticket) as total_price
FROM ticket_details
GROUP BY customer_id, aircraft_id WITH ROLLUP;

-- to create a view with only business class customers along with the brand of airlines
CREATE VIEW business_class_customers AS
SELECT c.first_name, c.last_name, t.brand
FROM customer c
JOIN ticket_details t ON c.customer_id = t.customer_id
WHERE t.class_id = 'Bussiness';

-- to create a stored procedure to get the details of all passengers flying between a range of routes defined in run time
-- Also, return an error message if the table doesn't exist
DELIMITER //

CREATE PROCEDURE get_passenger_detail (IN lower_bound_value INT, IN upper_bound_value INT)
BEGIN
    DECLARE tbl_name VARCHAR(50);
    
    SET tbl_name = 'passengers_on_flights';
    
    IF NOT EXISTS (SELECT table_name FROM information_schema.tables WHERE table_name = tbl_name) THEN
        SELECT CONCAT('Table "', tbl_name, '" does not exist') AS message;
    ELSE
        SELECT c.first_name, c.last_name, t.brand
        FROM customer c
        INNER JOIN ticket_details td ON c.customer_id = td.customer_id
        INNER JOIN passengers_on_flights pof ON td.ticket_id = pof.ticket_id
        INNER JOIN routes r ON pof.route_id = r.route_id
        INNER JOIN aircraft_id a ON pof.aircraft_id = a.aircraft_id
        INNER JOIN airlines t ON a.airline_id = t.airline_id
        WHERE r.route_id BETWEEN lower_bound_value AND upper_bound_value
        AND td.class_id = 'Business'
        GROUP BY c.customer_id, t.brand;
    END IF;
    
END//

DELIMITER ;

CALL get_passenger_detail(1, 25);

-- to create a stored procedure that extracts all the details from the routes table where the travelled distance is more than 2000 miles
DELIMITER //

CREATE PROCEDURE get_long_routes()
BEGIN
    SELECT *
    FROM routes
    WHERE distance_miles > 2000;
END //

DELIMITER ;

CALL get_long_routes();

-- to create a stored procedure that groups the distance travelled by each flight into three categories
DELIMITER //
CREATE PROCEDURE get_flight_distance_catgres()
BEGIN
    SELECT 
        aircraft_id,
        distance_miles,
        CASE
            WHEN distance_miles BETWEEN 0 AND 2000 THEN 'SDT'
            WHEN distance_miles > 2000 AND distance_miles <= 6500 THEN 'IDT'
            WHEN distance_miles > 6500 THEN 'LDT'
        END AS distance_category
    FROM routes;
END //
DELIMITER ;

CALL get_flight_distance_catgres();

--  to find if the complimentary services are provided for the specific class using a stored function in stored procedure on the ticket_details table
-- If the class is Business and Economy Plus, then complimentary services are given as Yes, else it is No
DELIMITER //
CREATE FUNCTION get_complimentary_services(class_id VARCHAR(20)) RETURNS VARCHAR(3) NO SQL
BEGIN
  DECLARE result VARCHAR(3);
  IF class_id IN ('Business', 'Economy Plus') THEN
    SET result = 'Yes';
  ELSE
    SET result = 'No';
  END IF;
  RETURN result;
END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE get_ticket_info()
BEGIN
  SELECT p_date, customer_id, class_id, get_complimentary_services(class_id) AS complimentary_services
  FROM ticket_details;
END //
DELIMITER ;

CALL get_ticket_info();

-- to extract the first record of the customer whose last name ends with Scott using a cursor from the customer table
DELIMITER //

CREATE PROCEDURE get_customer_scott()
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE cust_id INT;
  DECLARE cust_first_name VARCHAR(50);
  DECLARE cust_last_name VARCHAR(50);

  DECLARE cur_customer CURSOR FOR SELECT customer_id, first_name, last_name FROM customer WHERE last_name LIKE '%Scott' LIMIT 1;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  OPEN cur_customer;

  FETCH cur_customer INTO cust_id, cust_first_name, cust_last_name;

  IF NOT done THEN
    SELECT cust_id, cust_first_name, cust_last_name;
  END IF;

  CLOSE cur_customer;
END //

DELIMITER ;

CALL get_customer_scott();
